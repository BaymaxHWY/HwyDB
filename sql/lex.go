package sql

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

// 词法分析：将sql语句分割为token序列
// 还是要借鉴一下 learn-go-with-hard
type position struct {
	row int // 行
	col int // 列
}

type tokenType int

type token struct {
	typ tokenType // 记号
	lit string    // 对应值
	pos position // 当sql命令有多行时使用（暂时没用）
}

func (t token) String() string {
	return fmt.Sprintf("{%s : %q}\t", tokens[t.typ], t.lit)
}

const (
	KeyWord    tokenType = iota // 关键字
	Identifier                  // 标识符
	Literal                     //字面量(字符串)
	Num // 数字
	Symbol                      // 特殊符号
	Paren                       //括号( or )
	Semicolon                   //分号;
	EOF                         //结束
)

const eof = -1 // 用于判断str的字符有没有读完

var tokens = map[tokenType]string{
	KeyWord:    "关键字",
	Identifier: "标识符",
	Literal:    "字面量",
	Num: "数字",
	Symbol:     "特殊符号",
	Paren:      "括号",
	Semicolon:  "分号",
	EOF:        "结束",
}

// 用于状态之间的转换
type stateFuc func (l *lexer) stateFuc

type lexer struct {
	curToken *token    // 记录上一个token
	str      string   // 输入字符串
	start int // 字符串的起始位置
	pos int // 将要读到的位置
	width    int // 已经读取字符的size
	state stateFuc // 状态
	tokens   []*token // 解析到的token
	keyword map[string]bool // keyword
}

// 启动
func (l *lexer) run() {
	for l.state = lexBegin; l.state != nil; {
		l.state = l.state(l)
	}
}

// 获取下一个字符（字母、数字、中文、符号等）
func (l *lexer) next() rune {
	if l.pos >= len(l.str) {
		l.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(l.str[l.pos:]) // 总pos位置开始读
	l.width = w
	l.pos += l.width
	return r
}

// 跳过该字符
func (l *lexer) ignore() {
	l.start = l.pos
}

// 回到前一个字符
func (l *lexer) backup() {
	l.pos -= l.width
}

// 查看下一个字符
func (l *lexer) peek() rune {
	r := l.next()
	l.backup()
	return r
}

// 写入tokens
func (l *lexer) token(typ tokenType) {
	t := &token{
		typ: typ,
		lit: l.str[l.start : l.pos],
		pos: position{},
	}
	l.tokens = append(l.tokens, t)
	l.start = l.pos
	l.curToken = t
}

// 添加keyword
func (l *lexer) addKeyWord(keywords ...string) {
	for _, key := range keywords {
		l.keyword[key] = true
	}
}

// 入口函数
func NewLex(str string) *lexer {
	l :=  &lexer{
		str:str,
		keyword:make(map[string]bool),
	}
	l.addKeyWord("insert", "update", "delete", "find", "value", "into", "from", "where", "set")
	l.run()
	return l;
}
// 是否为英文字母
func isLetter(r rune) bool {
	r = unicode.ToLower(r)
	if r >= 'a' && r <= 'z' {
		return true
	}
	return false
}
// 是否为变量
func isVariable(r rune) bool {
 	if isLetter(r) || r == '_' {
 		return true
	}
	return false
}

// 是否为字面量（判断是否为引号）
func isLiteral(r rune) bool {
	if r == 34 || r == 39 {
		return true

	}
	return false
}

// 是否为特殊字符
func isSymbol(r rune) bool {
	symbols := map[string]int {
		"," : 1,
		"+" : 1,
		"-" : 1,
		"*" : 1,
		"/" : 1,
		"%" : 1,
		"=" : 1,
		"!" : 1,
		">" : 1,
		"<" : 1,
	}
	_, ok := symbols[string(r)]
	return ok
}

//-------------- state func ------------------------
func lexBegin(l *lexer) stateFuc {
	switch r := l.next(); {
	case unicode.IsDigit(r) || r == '.' || r == '-':
		if r == '-' && l.curToken.typ == Num {
			goto L //go to minus
		}
		l.backup()
		lexNum(l)
		return lexBegin
	L:
		fallthrough
	case unicode.IsSpace(r): // 空格就跳过
		l.ignore()
	case isVariable(r):
		return lexVariable
	case isLiteral(r):
		// 当前是引号可以跳过
		l.ignore()
		return lexLiteral
	case isSymbol(r):
		return lexSymbol
	case r == '(' || r == ')':
		return lexParen
	case r == ';':
		return lexSemicolon
	case r == eof:
		return lexEOF
	default:
		fmt.Println("this type is unkown")
		return lexUnkown
	}
	return lexBegin
}

func lexVariable(l *lexer) stateFuc {
	// 这里需要判断属于keyword还是标识符
	// 1.先读取后续的字符，直到读到非字母、数字、下划线
	for r := l.peek(); isVariable(r); {
		l.next()
		r = l.peek()
	}
	// 2.判断是否为keyword
	v := l.str[l.start:l.pos]
	if ok := l.keyword[v]; ok {
		return lexKeyWord
	}
	return lexIdentifier
}

func lexKeyWord(l *lexer) stateFuc {
	v := l.str[l.start:l.pos]
	if ok := l.keyword[v]; ok {
		l.token(KeyWord)
		return lexBegin
	}
	return nil
}

func lexIdentifier(l *lexer) stateFuc {
	l.token(Identifier)
	return lexBegin
}

func lexLiteral(l *lexer) stateFuc {
	// 读取字符串
	for r := l.peek(); unicode.IsLetter(r);{
		l.next()
		r = l.peek()
	}
	l.token(Literal)
	// 把收尾的引号消耗掉
	if isLiteral(l.peek()) {
		l.next()
	}
	return lexBegin
}

func lexNum(l *lexer) stateFuc {
	for r := l.peek(); unicode.IsDigit(r) || r == '.' || r == '-' || r == 'e'; {
		l.next()
		r = l.peek()
	}
	l.token(Num)
	return lexBegin
}

func lexSymbol(l *lexer) stateFuc {
	l.token(Symbol)
	return lexBegin
}

func lexParen(l *lexer) stateFuc {
	l.token(Paren)
	return lexBegin
}

func lexSemicolon(l *lexer) stateFuc {
	l.token(Semicolon)
	return nil
}

func lexEOF(l *lexer) stateFuc {
	return nil
}

func lexUnkown(l *lexer) stateFuc {
	return nil
}
