package conf

import "testing"

// Test to make sure we get what we expect.
func expect(t *testing.T, lx *lexer, items []item) {
	t.Helper()
	for i := 0; i < len(items); i++ {
		item := lx.nextItem()
		_ = item.String()
		if item.typ == itemEOF {
			break
		}
		if item != items[i] {
			t.Fatalf("Testing: '%s'\nExpected %q, received %q\n",
				lx.input, items[i], item)
		}
		if item.typ == itemError {
			break
		}
	}
}

func TestPlainValue(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyStringValues(t *testing.T) {
	// Double quotes
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "bar", 1, 7},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = \"bar\"")
	expect(t, lx, expectedItems)

	// Single quotes
	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "bar", 1, 7},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 'bar'")
	expect(t, lx, expectedItems)

	// No spaces
	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "bar", 1, 5},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo='bar'")
	expect(t, lx, expectedItems)

	// NL
	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "bar", 1, 5},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo='bar'\r\n")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "bar", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo=\t'bar'\t")
	expect(t, lx, expectedItems)
}

func TestComplexStringValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "bar\\r\\n  \\t", 1, 7},
		{itemEOF, "", 2, 0},
	}

	lx := lex("foo = 'bar\\r\\n  \\t'")
	expect(t, lx, expectedItems)
}

func TestStringStartingWithNumber(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "3xyz", 1, 6},
		{itemEOF, "", 2, 0},
	}

	lx := lex(`foo = 3xyz`)
	expect(t, lx, expectedItems)

	lx = lex(`foo = 3xyz,`)
	expect(t, lx, expectedItems)

	lx = lex(`foo = 3xyz;`)
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 2, 9},
		{itemString, "3xyz", 2, 15},
		{itemEOF, "", 2, 0},
	}
	content := `
        foo = 3xyz
        `
	lx = lex(content)
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "map", 2, 9},
		{itemMapStart, "", 2, 14},
		{itemKey, "foo", 3, 11},
		{itemString, "3xyz", 3, 17},
		{itemMapEnd, "", 3, 22},
		{itemEOF, "", 2, 0},
	}
	content = `
        map {
          foo = 3xyz}
        `
	lx = lex(content)
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "map", 2, 9},
		{itemMapStart, "", 2, 14},
		{itemKey, "foo", 3, 11},
		{itemString, "3xyz", 3, 17},
		{itemMapEnd, "", 4, 10},
		{itemEOF, "", 2, 0},
	}
	content = `
        map {
          foo = 3xyz;
        }
        `
	lx = lex(content)
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "map", 2, 9},
		{itemMapStart, "", 2, 14},
		{itemKey, "foo", 3, 11},
		{itemString, "3xyz", 3, 17},
		{itemKey, "bar", 4, 11},
		{itemString, "4wqs", 4, 17},
		{itemMapEnd, "", 5, 10},
		{itemEOF, "", 2, 0},
	}
	content = `
        map {
          foo = 3xyz,
          bar = 4wqs
        }
        `
	lx = lex(content)
	expect(t, lx, expectedItems)
}

func TestBinaryString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "e", 1, 9},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = \\x65")
	expect(t, lx, expectedItems)
}

func TestBinaryStringLatin1(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "\xe9", 1, 9},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = \\xe9")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyIntegerValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 4},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo=123")
	expect(t, lx, expectedItems)
	lx = lex("foo=123\r\n")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyNegativeIntegerValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "-123", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = -123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "-123", 1, 4},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo=-123")
	expect(t, lx, expectedItems)
	lx = lex("foo=-123\r\n")
	expect(t, lx, expectedItems)
}

func TestConvenientIntegerValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "1k", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = 1k")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "1K", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 1K")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "1m", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 1m")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "1M", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 1M")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "1g", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 1g")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "1G", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 1G")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "1MB", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 1MB")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "1Gb", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 1Gb")
	expect(t, lx, expectedItems)

	// Negative versions
	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "-1m", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = -1m")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "-1GB", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = -1GB ")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "1Ghz", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 1Ghz")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "2Pie", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 2Pie")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "3Mbs", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 3Mbs,")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "4Gb", 1, 6},
		{itemKey, "bar", 1, 11},
		{itemString, "5Gø", 1, 17},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 4Gb, bar = 5Gø")
	expect(t, lx, expectedItems)

	// A digit following a size suffix means this is not a convenient
	// number, so the whole token must be lexed as a string rather than
	// emitting a partial integer for the leading "0K". See #5186.
	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "0K1abc", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 0K1abc")
	expect(t, lx, expectedItems)

	// The 'e'/'E' suffix has the same problem: "5e7bcd" must be a string,
	// not the integer "5e7" followed by stray "bcd". See #5189 and #6891.
	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "5e7bcd", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 5e7bcd")
	expect(t, lx, expectedItems)

	// Same as above but terminated by a comma, ensuring the trailing
	// separator still ends the string value cleanly.
	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemString, "4e2abc", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = 4e2abc,")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyFloatValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemFloat, "22.2", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = 22.2")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemFloat, "22.2", 1, 4},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo=22.2")
	expect(t, lx, expectedItems)
	lx = lex("foo=22.2\r\n")
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringEndingAfterZeroHexChars(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemError, "Expected two hexadecimal digits after '\\x', but hit end of line", 2, 1},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = xyz\\x\n")
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringEndingAfterOneHexChar(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemError, "Expected two hexadecimal digits after '\\x', but hit end of line", 2, 1},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = xyz\\xF\n")
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringWithZeroHexChars(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemError, "Expected two hexadecimal digits after '\\x', but got ']\"'", 1, 12},
		{itemEOF, "", 1, 0},
	}
	lx := lex(`foo = "[\x]"`)
	expect(t, lx, expectedItems)
}

func TestBadBinaryStringWithOneHexChar(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemError, "Expected two hexadecimal digits after '\\x', but got 'e]'", 1, 12},
		{itemEOF, "", 1, 0},
	}
	lx := lex(`foo = "[\xe]"`)
	expect(t, lx, expectedItems)
}

func TestBadFloatValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemError, "Floats must start with a digit", 1, 7},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = .2")
	expect(t, lx, expectedItems)
}

func TestBadKey(t *testing.T) {
	expectedItems := []item{
		{itemError, "Unexpected key separator ':'", 1, 1},
		{itemEOF, "", 1, 0},
	}
	lx := lex(" :foo = 22")
	expect(t, lx, expectedItems)
}

func TestSimpleKeyBoolValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemBool, "true", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = true")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemBool, "true", 1, 4},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo=true")
	expect(t, lx, expectedItems)
	lx = lex("foo=true\r\n")
	expect(t, lx, expectedItems)
}

func TestComments(t *testing.T) {
	expectedItems := []item{
		{itemCommentStart, "", 1, 1},
		{itemText, " This is a comment", 1, 1},
		{itemEOF, "", 1, 0},
	}
	lx := lex("# This is a comment")
	expect(t, lx, expectedItems)
	lx = lex("# This is a comment\r\n")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemCommentStart, "", 1, 2},
		{itemText, " This is a comment", 1, 2},
		{itemEOF, "", 1, 0},
	}
	lx = lex("// This is a comment\r\n")
	expect(t, lx, expectedItems)
}

func TestTopValuesWithComments(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 6},
		{itemCommentStart, "", 1, 12},
		{itemText, " This is a comment", 1, 12},
		{itemEOF, "", 1, 0},
	}

	lx := lex("foo = 123 // This is a comment")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 4},
		{itemCommentStart, "", 1, 12},
		{itemText, " This is a comment", 1, 12},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo=123    # This is a comment")
	expect(t, lx, expectedItems)
}

func TestRawString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "bar", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = bar")
	expect(t, lx, expectedItems)
	lx = lex(`foo = bar' `)
	expect(t, lx, expectedItems)
}

func TestDateValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemDatetime, "2016-05-04T18:53:41Z", 1, 6},
		{itemEOF, "", 1, 0},
	}

	lx := lex("foo = 2016-05-04T18:53:41Z")
	expect(t, lx, expectedItems)
}

func TestVariableValues(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemVariable, "bar", 1, 7},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = $bar")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemVariable, "bar", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo =$bar")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemVariable, "bar", 1, 5},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo $bar")
	expect(t, lx, expectedItems)
}

func TestArrays(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemArrayStart, "", 1, 7},
		{itemInteger, "1", 1, 7},
		{itemInteger, "2", 1, 10},
		{itemInteger, "3", 1, 13},
		{itemString, "bar", 1, 17},
		{itemArrayEnd, "", 1, 22},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = [1, 2, 3, 'bar']")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemArrayStart, "", 1, 7},
		{itemInteger, "1", 1, 7},
		{itemInteger, "2", 1, 9},
		{itemInteger, "3", 1, 11},
		{itemString, "bar", 1, 14},
		{itemArrayEnd, "", 1, 19},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = [1,2,3,'bar']")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemArrayStart, "", 1, 7},
		{itemInteger, "1", 1, 7},
		{itemInteger, "2", 1, 10},
		{itemInteger, "3", 1, 12},
		{itemString, "bar", 1, 15},
		{itemArrayEnd, "", 1, 20},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo = [1, 2,3,'bar']")
	expect(t, lx, expectedItems)
}

var mlArray = `
# top level comment
foo = [
 1, # One
 2, // Two
 3 # Three
 'bar'     ,
 "bar"
]
`

func TestMultilineArrays(t *testing.T) {
	expectedItems := []item{
		{itemCommentStart, "", 2, 2},
		{itemText, " top level comment", 2, 2},
		{itemKey, "foo", 3, 1},
		{itemArrayStart, "", 3, 8},
		{itemInteger, "1", 4, 2},
		{itemCommentStart, "", 4, 6},
		{itemText, " One", 4, 6},
		{itemInteger, "2", 5, 2},
		{itemCommentStart, "", 5, 7},
		{itemText, " Two", 5, 7},
		{itemInteger, "3", 6, 2},
		{itemCommentStart, "", 6, 5},
		{itemText, " Three", 6, 5},
		{itemString, "bar", 7, 3},
		{itemString, "bar", 8, 3},
		{itemArrayEnd, "", 9, 2},
		{itemEOF, "", 9, 0},
	}
	lx := lex(mlArray)
	expect(t, lx, expectedItems)
}

var mlArrayNoSep = `
# top level comment
foo = [
 1 // foo
 2
 3
 'bar'
 "bar"
]
`

func TestMultilineArraysNoSep(t *testing.T) {
	expectedItems := []item{
		{itemCommentStart, "", 2, 2},
		{itemText, " top level comment", 2, 2},
		{itemKey, "foo", 3, 1},
		{itemArrayStart, "", 3, 8},
		{itemInteger, "1", 4, 2},
		{itemCommentStart, "", 4, 6},
		{itemText, " foo", 4, 6},
		{itemInteger, "2", 5, 2},
		{itemInteger, "3", 6, 2},
		{itemString, "bar", 7, 3},
		{itemString, "bar", 8, 3},
		{itemArrayEnd, "", 9, 2},
		{itemEOF, "", 9, 0},
	}
	lx := lex(mlArrayNoSep)
	expect(t, lx, expectedItems)
}

func TestSimpleMap(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemMapStart, "", 1, 7},
		{itemKey, "ip", 1, 7},
		{itemString, "127.0.0.1", 1, 11},
		{itemKey, "port", 1, 23},
		{itemInteger, "4242", 1, 30},
		{itemMapEnd, "", 1, 35},
		{itemEOF, "", 1, 0},
	}

	lx := lex("foo = {ip='127.0.0.1', port = 4242}")
	expect(t, lx, expectedItems)
}

var mlMap = `
foo = {
  ip = '127.0.0.1' # the IP
  port= 4242 // the port
}
`

func TestMultilineMap(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2, 1},
		{itemMapStart, "", 2, 8},
		{itemKey, "ip", 3, 3},
		{itemString, "127.0.0.1", 3, 9},
		{itemCommentStart, "", 3, 21},
		{itemText, " the IP", 3, 21},
		{itemKey, "port", 4, 3},
		{itemInteger, "4242", 4, 9},
		{itemCommentStart, "", 4, 16},
		{itemText, " the port", 4, 16},
		{itemMapEnd, "", 5, 2},
		{itemEOF, "", 5, 0},
	}

	lx := lex(mlMap)
	expect(t, lx, expectedItems)
}

var nestedMap = `
foo = {
  host = {
    ip = '127.0.0.1'
    port= 4242
  }
}
`

func TestNestedMaps(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2, 1},
		{itemMapStart, "", 2, 8},
		{itemKey, "host", 3, 3},
		{itemMapStart, "", 3, 11},
		{itemKey, "ip", 4, 5},
		{itemString, "127.0.0.1", 4, 11},
		{itemKey, "port", 5, 5},
		{itemInteger, "4242", 5, 11},
		{itemMapEnd, "", 6, 4},
		{itemMapEnd, "", 7, 2},
		{itemEOF, "", 7, 0},
	}

	lx := lex(nestedMap)
	expect(t, lx, expectedItems)
}

func TestQuotedKeys(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo : 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 1},
		{itemInteger, "123", 1, 8},
		{itemEOF, "", 1, 0},
	}
	lx = lex("'foo' : 123")
	expect(t, lx, expectedItems)
	lx = lex("\"foo\" : 123")
	expect(t, lx, expectedItems)
}

func TestQuotedKeysWithSpace(t *testing.T) {
	expectedItems := []item{
		{itemKey, " foo", 1, 1},
		{itemInteger, "123", 1, 9},
		{itemEOF, "", 1, 0},
	}
	lx := lex("' foo' : 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, " foo", 1, 1},
		{itemInteger, "123", 1, 9},
		{itemEOF, "", 1, 0},
	}
	lx = lex("\" foo\" : 123")
	expect(t, lx, expectedItems)
}

func TestColonKeySep(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo : 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 4},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo:123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 5},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo: 123")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 6},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo:  123\r\n")
	expect(t, lx, expectedItems)
}

func TestWhitespaceKeySep(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 4},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo 123")
	expect(t, lx, expectedItems)
	lx = lex("foo 123")
	expect(t, lx, expectedItems)
	lx = lex("foo\t123")
	expect(t, lx, expectedItems)
	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemInteger, "123", 1, 5},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo\t\t123\r\n")
	expect(t, lx, expectedItems)
}

var escString = `
foo  = \t
bar  = \r
baz  = \n
q    = \"
bs   = \\
`

func TestEscapedString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2, 1},
		{itemString, "\t", 2, 9},
		{itemKey, "bar", 3, 1},
		{itemString, "\r", 3, 9},
		{itemKey, "baz", 4, 1},
		{itemString, "\n", 4, 9},
		{itemKey, "q", 5, 1},
		{itemString, "\"", 5, 9},
		{itemKey, "bs", 6, 1},
		{itemString, "\\", 6, 9},
		{itemEOF, "", 6, 0},
	}
	lx := lex(escString)
	expect(t, lx, expectedItems)
}

func TestCompoundStringES(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "\\end", 1, 8},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = "\\end"`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringSE(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "start\\", 1, 8},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = "start\\"`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringEE(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "Eq", 1, 12},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = \x45\x71`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringSEE(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "startEq", 1, 12},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = start\x45\x71`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringSES(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "start|end", 1, 9},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = start\x7Cend`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringEES(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "<>end", 1, 12},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = \x3c\x3eend`)
	expect(t, lx, expectedItems)
}

func TestCompoundStringESE(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "<middle>", 1, 12},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = \x3cmiddle\x3E`)
	expect(t, lx, expectedItems)
}

func TestBadStringEscape(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemError, "Invalid escape character 'y'. Only the following escape characters are allowed: \\xXX, \\t, \\n, \\r, \\\", \\\\.", 1, 8},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = \y`)
	expect(t, lx, expectedItems)
}

func TestNonBool(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "\\true", 1, 7},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = \\true`)
	expect(t, lx, expectedItems)
}

func TestNonVariable(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "\\$var", 1, 7},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = \\$var`)
	expect(t, lx, expectedItems)
}

func TestEmptyStringDQ(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "", 1, 7},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = ""`)
	expect(t, lx, expectedItems)
}

func TestEmptyStringSQ(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "", 1, 7},
		{itemEOF, "", 2, 0},
	}
	lx := lex(`foo = ''`)
	expect(t, lx, expectedItems)
}

var nestedWhitespaceMap = `
foo  {
  host  {
    ip = '127.0.0.1'
    port= 4242
  }
}
`

func TestNestedWhitespaceMaps(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2, 1},
		{itemMapStart, "", 2, 7},
		{itemKey, "host", 3, 3},
		{itemMapStart, "", 3, 10},
		{itemKey, "ip", 4, 5},
		{itemString, "127.0.0.1", 4, 11},
		{itemKey, "port", 5, 5},
		{itemInteger, "4242", 5, 11},
		{itemMapEnd, "", 6, 4},
		{itemMapEnd, "", 7, 2},
		{itemEOF, "", 7, 0},
	}

	lx := lex(nestedWhitespaceMap)
	expect(t, lx, expectedItems)
}

var semicolons = `
foo = 123;
bar = 'baz';
baz = 'boo'
map {
 id = 1;
}
`

func TestOptionalSemicolons(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2, 1},
		{itemInteger, "123", 2, 7},
		{itemKey, "bar", 3, 1},
		{itemString, "baz", 3, 8},
		{itemKey, "baz", 4, 1},
		{itemString, "boo", 4, 8},
		{itemKey, "map", 5, 1},
		{itemMapStart, "", 5, 6},
		{itemKey, "id", 6, 2},
		{itemInteger, "1", 6, 7},
		{itemMapEnd, "", 7, 2},
		{itemEOF, "", 8, 0},
	}

	lx := lex(semicolons)
	expect(t, lx, expectedItems)
}

func TestSemicolonChaining(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemString, "1", 1, 5},
		{itemKey, "bar", 1, 9},
		{itemFloat, "2.2", 1, 13},
		{itemKey, "baz", 1, 18},
		{itemBool, "true", 1, 22},
		{itemEOF, "", 1, 0},
	}

	lx := lex("foo='1'; bar=2.2; baz=true;")
	expect(t, lx, expectedItems)
}

var noquotes = `
foo = 123
bar = baz
baz=boo
map {
 id:one
 id2 : onetwo
}
t true
f false
tstr "true"
tkey = two
fkey = five # This should be a string
`

func TestNonQuotedStrings(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2, 1},
		{itemInteger, "123", 2, 7},
		{itemKey, "bar", 3, 1},
		{itemString, "baz", 3, 7},
		{itemKey, "baz", 4, 1},
		{itemString, "boo", 4, 5},
		{itemKey, "map", 5, 1},
		{itemMapStart, "", 5, 6},
		{itemKey, "id", 6, 2},
		{itemString, "one", 6, 5},
		{itemKey, "id2", 7, 2},
		{itemString, "onetwo", 7, 8},
		{itemMapEnd, "", 8, 2},
		{itemKey, "t", 9, 1},
		{itemBool, "true", 9, 3},
		{itemKey, "f", 10, 1},
		{itemBool, "false", 10, 3},
		{itemKey, "tstr", 11, 1},
		{itemString, "true", 11, 7},
		{itemKey, "tkey", 12, 1},
		{itemString, "two", 12, 8},
		{itemKey, "fkey", 13, 1},
		{itemString, "five", 13, 8},
		{itemCommentStart, "", 13, 14},
		{itemText, " This should be a string", 13, 14},
		{itemEOF, "", 14, 0},
	}
	lx := lex(noquotes)
	expect(t, lx, expectedItems)
}

var danglingquote = `
listen: "localhost:4242

http: localhost:8222
`

func TestDanglingQuotedString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "listen", 2, 1},
		{itemError, "Unexpected EOF.", 5, 1},
	}
	lx := lex(danglingquote)
	expect(t, lx, expectedItems)
}

var keydanglingquote = `
foo = "
listen: "

http: localhost:8222

"
`

func TestKeyDanglingQuotedString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2, 1},
		{itemString, "\nlisten: ", 3, 8},
		{itemKey, "http", 5, 1},
		{itemString, "localhost:8222", 5, 7},
		{itemError, "Unexpected EOF.", 8, 1},
	}
	lx := lex(keydanglingquote)
	expect(t, lx, expectedItems)
}

var danglingsquote = `
listen: 'localhost:4242

http: localhost:8222
`

func TestDanglingSingleQuotedString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "listen", 2, 1},
		{itemError, "Unexpected EOF.", 5, 1},
	}
	lx := lex(danglingsquote)
	expect(t, lx, expectedItems)
}

var keydanglingsquote = `
foo = '
listen: '

http: localhost:8222

'
`

func TestKeyDanglingSingleQuotedString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 2, 1},
		{itemString, "\nlisten: ", 3, 8},
		{itemKey, "http", 5, 1},
		{itemString, "localhost:8222", 5, 7},
		{itemError, "Unexpected EOF.", 8, 1},
	}
	lx := lex(keydanglingsquote)
	expect(t, lx, expectedItems)
}

var mapdanglingbracket = `
listen = 4222

cluster = {

  foo = bar

`

func TestMapDanglingBracket(t *testing.T) {
	expectedItems := []item{
		{itemKey, "listen", 2, 1},
		{itemInteger, "4222", 2, 10},
		{itemKey, "cluster", 4, 1},
		{itemMapStart, "", 4, 12},
		{itemKey, "foo", 6, 3},
		{itemString, "bar", 6, 9},
		{itemError, "Unexpected EOF processing map.", 8, 1},
	}
	lx := lex(mapdanglingbracket)
	expect(t, lx, expectedItems)
}

var blockdanglingparens = `
listen = 4222

quote = (

  foo = bar

`

func TestBlockDanglingParens(t *testing.T) {
	expectedItems := []item{
		{itemKey, "listen", 2, 1},
		{itemInteger, "4222", 2, 10},
		{itemKey, "quote", 4, 1},
		{itemError, "Unexpected EOF processing block.", 8, 1},
	}
	lx := lex(blockdanglingparens)
	expect(t, lx, expectedItems)
}

func TestMapQuotedKeys(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemMapStart, "", 1, 7},
		{itemKey, "bar", 1, 8},
		{itemInteger, "4242", 1, 15},
		{itemMapEnd, "", 1, 20},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = {'bar' = 4242}")
	expect(t, lx, expectedItems)
	lx = lex("foo = {\"bar\" = 4242}")
	expect(t, lx, expectedItems)
}

func TestSpecialCharsMapQuotedKeys(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemMapStart, "", 1, 7},
		{itemKey, "bar-1.2.3", 1, 8},
		{itemMapStart, "", 1, 22},
		{itemKey, "port", 1, 23},
		{itemInteger, "4242", 1, 28},
		{itemMapEnd, "", 1, 34},
		{itemMapEnd, "", 1, 35},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo = {'bar-1.2.3' = { port:4242 }}")
	expect(t, lx, expectedItems)
	lx = lex("foo = {\"bar-1.2.3\" = { port:4242 }}")
	expect(t, lx, expectedItems)
}

var mlnestedmap = `
systems {
  allinone {
    description: "This is a description."
  }
}
`

func TestDoubleNestedMapsNewLines(t *testing.T) {
	expectedItems := []item{
		{itemKey, "systems", 2, 1},
		{itemMapStart, "", 2, 10},
		{itemKey, "allinone", 3, 3},
		{itemMapStart, "", 3, 13},
		{itemKey, "description", 4, 5},
		{itemString, "This is a description.", 4, 19},
		{itemMapEnd, "", 5, 4},
		{itemMapEnd, "", 6, 2},
		{itemEOF, "", 7, 0},
	}
	lx := lex(mlnestedmap)
	expect(t, lx, expectedItems)
}

var blockexample = `
numbers (
1234567890
)
`

func TestBlockString(t *testing.T) {
	expectedItems := []item{
		{itemKey, "numbers", 2, 1},
		{itemString, "\n1234567890\n", 4, 10},
	}
	lx := lex(blockexample)
	expect(t, lx, expectedItems)
}

func TestBlockStringEOF(t *testing.T) {
	expectedItems := []item{
		{itemKey, "numbers", 2, 1},
		{itemString, "\n1234567890\n", 4, 10},
	}
	blockbytes := []byte(blockexample[0 : len(blockexample)-1])
	blockbytes = append(blockbytes, 0)
	lx := lex(string(blockbytes))
	expect(t, lx, expectedItems)
}

var mlblockexample = `
numbers (
  12(34)56
  (
    7890
  )
)
`

func TestBlockStringMultiLine(t *testing.T) {
	expectedItems := []item{
		{itemKey, "numbers", 2, 1},
		{itemString, "\n  12(34)56\n  (\n    7890\n  )\n", 7, 10},
	}
	lx := lex(mlblockexample)
	expect(t, lx, expectedItems)
}

func TestUnquotedIPAddr(t *testing.T) {
	expectedItems := []item{
		{itemKey, "listen", 1, 0},
		{itemString, "127.0.0.1:4222", 1, 8},
		{itemEOF, "", 1, 0},
	}
	lx := lex("listen: 127.0.0.1:4222")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1, 0},
		{itemString, "127.0.0.1", 1, 8},
		{itemEOF, "", 1, 0},
	}
	lx = lex("listen: 127.0.0.1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1, 0},
		{itemString, "apcera.me:80", 1, 8},
		{itemEOF, "", 1, 0},
	}
	lx = lex("listen: apcera.me:80")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1, 0},
		{itemString, "nats.io:-1", 1, 8},
		{itemEOF, "", 1, 0},
	}
	lx = lex("listen: nats.io:-1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1, 0},
		{itemInteger, "-1", 1, 8},
		{itemEOF, "", 1, 0},
	}
	lx = lex("listen: -1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1, 0},
		{itemString, ":-1", 1, 8},
		{itemEOF, "", 1, 0},
	}
	lx = lex("listen: :-1")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1, 0},
		{itemString, ":80", 1, 9},
		{itemEOF, "", 1, 0},
	}
	lx = lex("listen = :80")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "listen", 1, 0},
		{itemArrayStart, "", 1, 10},
		{itemString, "localhost:4222", 1, 10},
		{itemString, "localhost:4333", 1, 26},
		{itemArrayEnd, "", 1, 41},
		{itemEOF, "", 1, 0},
	}
	lx = lex("listen = [localhost:4222, localhost:4333]")
	expect(t, lx, expectedItems)
}

var arrayOfMaps = `
authorization {
    users = [
      {user: alice, password: foo}
      {user: bob,   password: bar}
    ]
    timeout: 0.5
}
`

func TestArrayOfMaps(t *testing.T) {
	expectedItems := []item{
		{itemKey, "authorization", 2, 1},
		{itemMapStart, "", 2, 16},
		{itemKey, "users", 3, 5},
		{itemArrayStart, "", 3, 14},
		{itemMapStart, "", 4, 8},
		{itemKey, "user", 4, 8},
		{itemString, "alice", 4, 14},
		{itemKey, "password", 4, 21},
		{itemString, "foo", 4, 31},
		{itemMapEnd, "", 4, 35},
		{itemMapStart, "", 5, 8},
		{itemKey, "user", 5, 8},
		{itemString, "bob", 5, 14},
		{itemKey, "password", 5, 21},
		{itemString, "bar", 5, 31},
		{itemMapEnd, "", 5, 35},
		{itemArrayEnd, "", 6, 6},
		{itemKey, "timeout", 7, 5},
		{itemFloat, "0.5", 7, 14},
		{itemMapEnd, "", 8, 2},
		{itemEOF, "", 9, 0},
	}
	lx := lex(arrayOfMaps)
	expect(t, lx, expectedItems)
}

func TestInclude(t *testing.T) {
	expectedItems := []item{
		{itemInclude, "users.conf", 1, 9},
		{itemEOF, "", 1, 0},
	}
	lx := lex("include \"users.conf\"")
	expect(t, lx, expectedItems)

	lx = lex("include 'users.conf'")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemInclude, "users.conf", 1, 8},
		{itemEOF, "", 1, 0},
	}
	lx = lex("include users.conf")
	expect(t, lx, expectedItems)
}

func TestOptionalInclude(t *testing.T) {
	expectedItems := []item{
		{itemIncludeOptional, "users.conf", 1, 10},
		{itemEOF, "", 1, 0},
	}
	lx := lex("include? \"users.conf\"")
	expect(t, lx, expectedItems)

	lx = lex("include? 'users.conf'")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemIncludeOptional, "users.conf", 1, 9},
		{itemEOF, "", 1, 0},
	}
	lx = lex("include? users.conf")
	expect(t, lx, expectedItems)

	// An include after an optional one is still a regular include.
	expectedItems = []item{
		{itemIncludeOptional, "a.conf", 1, 9},
		{itemInclude, "b.conf", 2, 9},
		{itemEOF, "", 2, 0},
	}
	lx = lex("include? a.conf\ninclude b.conf")
	expect(t, lx, expectedItems)
}

func TestMapOptionalInclude(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemMapStart, "", 1, 5},
		{itemIncludeOptional, "users.conf", 1, 15},
		{itemMapEnd, "", 1, 27},
		{itemEOF, "", 1, 0},
	}
	lx := lex("foo { include? users.conf }")
	expect(t, lx, expectedItems)
}

func TestMapInclude(t *testing.T) {
	expectedItems := []item{
		{itemKey, "foo", 1, 0},
		{itemMapStart, "", 1, 5},
		{itemInclude, "users.conf", 1, 14},
		{itemMapEnd, "", 1, 26},
		{itemEOF, "", 1, 0},
	}

	lx := lex("foo { include users.conf }")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemMapStart, "", 1, 5},
		{itemInclude, "users.conf", 1, 13},
		{itemMapEnd, "", 1, 24},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo {include users.conf}")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemMapStart, "", 1, 5},
		{itemInclude, "users.conf", 1, 15},
		{itemMapEnd, "", 1, 28},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo { include 'users.conf' }")
	expect(t, lx, expectedItems)

	expectedItems = []item{
		{itemKey, "foo", 1, 0},
		{itemMapStart, "", 1, 5},
		{itemInclude, "users.conf", 1, 15},
		{itemMapEnd, "", 1, 27},
		{itemEOF, "", 1, 0},
	}
	lx = lex("foo { include \"users.conf\"}")
	expect(t, lx, expectedItems)
}

func TestJSONCompat(t *testing.T) {
	for _, test := range []struct {
		name     string
		input    string
		expected []item
	}{
		{
			name: "should omit initial and final brackets at top level with a single item",
			input: `
                        {
                          "http_port": 8223
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 3, 28},
				{itemInteger, "8223", 3, 40},
				{itemKey, "}", 4, 25},
				{itemEOF, "", 0, 0},
			},
		},
		{
			name: "should omit trailing commas at top level with two items",
			input: `
                        {
                          "http_port": 8223,
                          "port": 4223
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 3, 28},
				{itemInteger, "8223", 3, 40},
				{itemKey, "port", 4, 28},
				{itemInteger, "4223", 4, 35},
				{itemKey, "}", 5, 25},
				{itemEOF, "", 0, 0},
			},
		},
		{
			name: "should omit trailing commas at top level with multiple items",
			input: `
                        {
                          "http_port": 8223,
                          "port": 4223,
                          "max_payload": "5MB",
                          "debug": true,
                          "max_control_line": 1024
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 3, 28},
				{itemInteger, "8223", 3, 40},
				{itemKey, "port", 4, 28},
				{itemInteger, "4223", 4, 35},
				{itemKey, "max_payload", 5, 28},
				{itemString, "5MB", 5, 43},
				{itemKey, "debug", 6, 28},
				{itemBool, "true", 6, 36},
				{itemKey, "max_control_line", 7, 28},
				{itemInteger, "1024", 7, 47},
				{itemKey, "}", 8, 25},
				{itemEOF, "", 0, 0},
			},
		},
		{
			name: "should support JSON not prettified",
			input: `{"http_port": 8224,"port": 4224}
                        `,
			expected: []item{
				{itemKey, "http_port", 1, 2},
				{itemInteger, "8224", 1, 14},
				{itemKey, "port", 1, 20},
				{itemInteger, "4224", 1, 27},
				{itemEOF, "", 0, 0},
			},
		},
		{
			name: "should support JSON not prettified with final bracket after newline",
			input: `{"http_port": 8225,"port": 4225
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 1, 2},
				{itemInteger, "8225", 1, 14},
				{itemKey, "port", 1, 20},
				{itemInteger, "4225", 1, 27},
				{itemKey, "}", 2, 25},
				{itemEOF, "", 0, 0},
			},
		},
		{
			name: "should support uglified JSON with inner blocks",
			input: `{"http_port": 8227,"port": 4227,"write_deadline": "1h","cluster": {"port": 6222,"routes": ["nats://127.0.0.1:4222","nats://127.0.0.1:4223","nats://127.0.0.1:4224"]}}
                        `,
			expected: []item{
				{itemKey, "http_port", 1, 2},
				{itemInteger, "8227", 1, 14},
				{itemKey, "port", 1, 20},
				{itemInteger, "4227", 1, 27},
				{itemKey, "write_deadline", 1, 33},
				{itemString, "1h", 1, 51},
				{itemKey, "cluster", 1, 56},
				{itemMapStart, "", 1, 67},
				{itemKey, "port", 1, 68},
				{itemInteger, "6222", 1, 75},
				{itemKey, "routes", 1, 81},
				{itemArrayStart, "", 1, 91},
				{itemString, "nats://127.0.0.1:4222", 1, 92},
				{itemString, "nats://127.0.0.1:4223", 1, 116},
				{itemString, "nats://127.0.0.1:4224", 1, 140},
				{itemArrayEnd, "", 1, 163},
				{itemMapEnd, "", 1, 164},
				{itemKey, "}", 14, 25},
				{itemEOF, "", 0, 0},
			},
		},
		{
			name: "should support prettified JSON with inner blocks",
			input: `
                        {
                          "http_port": 8227,
                          "port": 4227,
                          "write_deadline": "1h",
                          "cluster": {
                            "port": 6222,
                            "routes": [
                              "nats://127.0.0.1:4222",
                              "nats://127.0.0.1:4223",
                              "nats://127.0.0.1:4224"
                            ]
                          }
                        }
                        `,
			expected: []item{
				{itemKey, "http_port", 3, 28},
				{itemInteger, "8227", 3, 40},
				{itemKey, "port", 4, 28},
				{itemInteger, "4227", 4, 35},
				{itemKey, "write_deadline", 5, 28},
				{itemString, "1h", 5, 46},
				{itemKey, "cluster", 6, 28},
				{itemMapStart, "", 6, 39},
				{itemKey, "port", 7, 30},
				{itemInteger, "6222", 7, 37},
				{itemKey, "routes", 8, 30},
				{itemArrayStart, "", 8, 40},
				{itemString, "nats://127.0.0.1:4222", 9, 32},
				{itemString, "nats://127.0.0.1:4223", 10, 32},
				{itemString, "nats://127.0.0.1:4224", 11, 32},
				{itemArrayEnd, "", 12, 30},
				{itemMapEnd, "", 13, 28},
				{itemKey, "}", 14, 25},
				{itemEOF, "", 0, 0},
			},
		},
		{
			name: "should support JSON with blocks",
			input: `{
                          "jetstream": {
                            "store_dir": "/tmp/nats"
                            "max_mem": 1000000,
                          },
                          "port": 4222,
                          "server_name": "nats1"
                        }
                        `,
			expected: []item{
				{itemKey, "jetstream", 2, 28},
				{itemMapStart, "", 2, 41},
				{itemKey, "store_dir", 3, 30},
				{itemString, "/tmp/nats", 3, 43},
				{itemKey, "max_mem", 4, 30},
				{itemInteger, "1000000", 4, 40},
				{itemMapEnd, "", 5, 28},
				{itemKey, "port", 6, 28},
				{itemInteger, "4222", 6, 35},
				{itemKey, "server_name", 7, 28},
				{itemString, "nats1", 7, 43},
				{itemKey, "}", 8, 25},
				{itemEOF, "", 0, 0},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			lx := lex(test.input)
			expect(t, lx, test.expected)
		})
	}
}
