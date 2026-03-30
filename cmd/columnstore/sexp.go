package main

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/nordfjord/columnstore"
)

type Kind uint8

const (
	KindAtom Kind = iota + 1
	KindList
)

type Sexp struct {
	Kind Kind
	Atom string
	List []Sexp
}

func (s Sexp) IsAtom() bool {
	return s.Kind == KindAtom
}

func (s Sexp) IsList() bool {
	return s.Kind == KindList
}

func Atom(value string) Sexp {
	return Sexp{Kind: KindAtom, Atom: value}
}

func List(items ...Sexp) Sexp {
	return Sexp{Kind: KindList, List: items}
}

func Parse(stream string) (Sexp, error) {
	p := parser{stream: stream}
	expr, err := p.expr()
	if err != nil {
		return Sexp{}, err
	}
	p.skipWhitespace()
	if p.peek() != 0 {
		return Sexp{}, p.errorf("superfluous characters after expression: %q", string(p.peek()))
	}
	return expr, nil
}

func mustParseSexp(stream string) Sexp {
	expr, err := Parse(stream)
	if err != nil {
		panic(err)
	}
	return expr
}

func (s Sexp) String() string {
	if s.Kind == KindList {
		parts := make([]string, 0, len(s.List))
		for _, item := range s.List {
			parts = append(parts, item.String())
		}
		return "(" + strings.Join(parts, " ") + ")"
	}

	if needsQuotes(s.Atom) {
		return strconv.Quote(s.Atom)
	}

	return s.Atom
}

type SyntaxError struct {
	Message string
	Line    int
	Column  int
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("syntax error at %d:%d: %s", e.Line, e.Column, e.Message)
}

type parser struct {
	stream string
	pos    int
	line   int
	col    int
}

func (p *parser) expr() (Sexp, error) {
	p.skipWhitespace()
	if p.peek() == 0 {
		return Sexp{}, p.errorf("expected expression")
	}
	if p.peek() == '(' {
		return p.list()
	}
	return p.atom()
}

func (p *parser) list() (Sexp, error) {
	if p.peek() != '(' {
		return Sexp{}, p.errorf("expected '(', saw %q", string(p.peek()))
	}
	p.consume()

	var items []Sexp
	for {
		p.skipWhitespace()
		next := p.peek()
		switch next {
		case 0:
			return Sexp{}, p.errorf("unterminated list")
		case ')':
			p.consume()
			return List(items...), nil
		default:
			item, err := p.expr()
			if err != nil {
				return Sexp{}, err
			}
			items = append(items, item)
		}
	}

}

func (p *parser) atom() (Sexp, error) {
	if p.peek() == '"' {
		value, err := p.string()
		if err != nil {
			return Sexp{}, err
		}
		return Atom(value), nil
	}

	var b strings.Builder
	for {
		next := p.peek()
		switch {
		case next == 0 || unicode.IsSpace(rune(next)) || next == '(' || next == ')':
			if b.Len() == 0 {
				return Sexp{}, p.errorf("expected atom")
			}
			return Atom(b.String()), nil
		case next == '\\':
			p.consume()
			escaped := p.peek()
			if escaped == 0 {
				return Sexp{}, p.errorf("unterminated escape")
			}
			b.WriteByte(p.consume())
		default:
			b.WriteByte(p.consume())
		}
	}
}

func (p *parser) string() (string, error) {
	if p.peek() != '"' {
		return "", p.errorf("expected string")
	}
	p.consume()

	var b strings.Builder
	for {
		next := p.peek()
		switch next {
		case 0:
			return "", p.errorf("unterminated string literal")
		case '"':
			p.consume()
			return b.String(), nil
		case '\\':
			p.consume()
			escaped := p.peek()
			switch escaped {
			case 0:
				return "", p.errorf("unterminated string escape")
			case 'r':
				p.consume()
				b.WriteByte('\r')
			case 't':
				p.consume()
				b.WriteByte('\t')
			case 'n':
				p.consume()
				b.WriteByte('\n')
			case 'f':
				p.consume()
				b.WriteByte('\f')
			case 'b':
				p.consume()
				b.WriteByte('\b')
			default:
				b.WriteByte(p.consume())
			}
		default:
			b.WriteByte(p.consume())
		}
	}
}

func (p *parser) skipWhitespace() {
	for {
		next := p.peek()
		if next == 0 || !unicode.IsSpace(rune(next)) {
			return
		}
		p.consume()
	}
}

func (p *parser) peek() byte {
	if p.pos >= len(p.stream) {
		return 0
	}
	return p.stream[p.pos]
}

func (p *parser) consume() byte {
	if p.pos >= len(p.stream) {
		return 0
	}

	c := p.stream[p.pos]
	p.pos++

	if c == '\r' {
		if p.peek() == '\n' {
			p.pos++
		}
		p.line++
		p.col = 0
		return c
	}

	if c == '\n' {
		p.line++
		p.col = 0
		return c
	}

	p.col++
	return c
}

func (p *parser) errorf(format string, args ...any) error {
	return &SyntaxError{
		Message: fmt.Sprintf(format, args...),
		Line:    p.line + 1,
		Column:  p.col + 1,
	}
}

func needsQuotes(s string) bool {
	if s == "" {
		return true
	}
	for i := range len(s) {
		c := s[i]
		if unicode.IsSpace(rune(c)) || c == '(' || c == ')' || c == '"' || c == '\\' {
			return true
		}
	}
	return false
}

func filterFromSexp(sexp Sexp) (columnstore.Filter, error) {
	if !sexp.IsList() {
		return nil, fmt.Errorf("expected list, got %s", sexp.String())
	}
	if len(sexp.List) < 2 {
		return nil, fmt.Errorf("expected at least two elements, got %d", len(sexp.List))
	}

	op := sexp.List[0]
	if !op.IsAtom() {
		return nil, fmt.Errorf("expected atom, got %s", op.String())
	}
	switch op.Atom {
	case "and":
		sexps := sexp.List[1:]
		if len(sexps) == 0 {
			return nil, fmt.Errorf("AND filter requires at least one expr")
		}
		exprs := make([]columnstore.Filter, 0, len(sexps))
		for _, sexp := range sexps {
			expr, err := filterFromSexp(sexp)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
		}
		return columnstore.And(exprs...), nil
	case "or":
		sexps := sexp.List[1:]
		if len(sexps) == 0 {
			return nil, fmt.Errorf("OR filter requires at least one expr")
		}
		exprs := make([]columnstore.Filter, 0, len(sexps))
		for _, sexp := range sexps {
			expr, err := filterFromSexp(sexp)
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expr)
		}
		return columnstore.Or(exprs...), nil
	case "eq":
		if len(sexp.List) != 3 {
			return nil, fmt.Errorf("Eq filter requires field and value")
		}
		field := sexp.List[1]
		value := sexp.List[2]
		if !field.IsAtom() {
			return nil, fmt.Errorf("expected atom, got %s", field.String())
		}
		if !value.IsAtom() {
			return nil, fmt.Errorf("expected atom, got %s", value.String())
		}
		return columnstore.Eq(field.Atom, parseFilterValue(value.Atom)), nil
	case "exists", "not-exists":
		if len(sexp.List) != 2 {
			return nil, fmt.Errorf("%s filter requires field", op.Atom)
		}
		field := sexp.List[1]
		if !field.IsAtom() {
			return nil, fmt.Errorf("expected atom, got %s", field.String())
		}
		if op.Atom == "exists" {
			return columnstore.Exists(field.Atom), nil
		}
		return columnstore.NotExists(field.Atom), nil
	case "in", "not-in":
		if len(sexp.List) != 3 {
			return nil, fmt.Errorf("%s filter requires field and value", op.Atom)
		}
		field := sexp.List[1]
		value := sexp.List[2]
		if !field.IsAtom() {
			return nil, fmt.Errorf("expected atom, got %s", field.String())
		}
		if !value.IsList() {
			return nil, fmt.Errorf("expected list, got %s", value.String())
		}
		values := make([]any, 0, len(value.List))
		for _, v := range value.List {
			if !v.IsAtom() {
				return nil, fmt.Errorf("expected atom, got %s", v.String())
			}
			values = append(values, v.Atom)
		}
		if op.Atom == "in" {
			return columnstore.In(field.Atom, values...), nil
		}
		return columnstore.NotIn(field.Atom, values...), nil
	case "lt", "lte", "gt", "gte":
		if len(sexp.List) != 3 {
			return nil, fmt.Errorf("%s filter requires field and value", op.Atom)
		}
		field := sexp.List[1]
		value := sexp.List[2]
		if !field.IsAtom() {
			return nil, fmt.Errorf("expected atom, got %s", field.String())
		}
		if !value.IsAtom() {
			return nil, fmt.Errorf("expected atom, got %s", value.String())
		}
		val := parseFilterValue(value.Atom)
		switch op.Atom {
		case "lt":
			switch value := val.(type) {
			case int64:
				return columnstore.Lt(field.Atom, value), nil
			case float64:
				return columnstore.Lt(field.Atom, value), nil
			case string:
				return columnstore.Lt(field.Atom, value), nil
			default:
				return nil, fmt.Errorf("unsupported filter type %q", op.Atom)
			}
		case "lte":
			switch value := val.(type) {
			case int64:
				return columnstore.Lte(field.Atom, value), nil
			case float64:
				return columnstore.Lte(field.Atom, value), nil
			case string:
				return columnstore.Lte(field.Atom, value), nil
			default:
				return nil, fmt.Errorf("unsupported filter type %q", op.Atom)
			}
		case "gt":
			switch value := val.(type) {
			case int64:
				return columnstore.Gt(field.Atom, value), nil
			case float64:
				return columnstore.Gt(field.Atom, value), nil
			case string:
				return columnstore.Gt(field.Atom, value), nil
			default:
				return nil, fmt.Errorf("unsupported filter type %q", op.Atom)
			}
		case "gte":
			switch value := val.(type) {
			case int64:
				return columnstore.Gte(field.Atom, value), nil
			case float64:
				return columnstore.Gte(field.Atom, value), nil
			case string:
				return columnstore.Gte(field.Atom, value), nil
			default:
				return nil, fmt.Errorf("unsupported filter type %q", op.Atom)
			}
		default:
			return nil, fmt.Errorf("unsupported filter type %q", op.Atom)
		}
	default:
		return nil, fmt.Errorf("unsupported filter type %q", op.Atom)
	}
}

func parseFilterValue(value string) any {
	// try to parse as int64
	i, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		return i
	}
	f, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return f
	}
	if value == "true" || value == "false" {
		return value == "true"
	}
	// fallback to string
	return value
}
