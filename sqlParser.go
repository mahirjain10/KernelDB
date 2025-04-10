package main

import (
	"math"
)

type QLNode struct {
	Type uint32 // tagged union
	I64  int64
	Str  []byte
	Kids []QLNode // operands
}

//  statements : select,update,delete

type QLSelect struct {
	QLScan
	Names  []string // expression as Name
	Output []QLNode
}

type QLUpdate struct {
	QLScan
	Names  []string
	Values []QLNode
}
type QLDelete struct {
	QLScan
}

// common structure for statements : `INDEX BY`,`FILTER`,`LIMIT`

type QLScan struct {
	Table  string // table name
	Key1   QLNode // starting key
	Key2   QLNode // ending key
	Filter QLNode // filter expression // An expression tree (e.g., age > 18 in WHERE age > 18).
	Offset int64  // how many rows to skip
	Limit  int64  //  how many rows to return
}

type QLEvalContex struct {
	env Record // input row (the current row being processed)
	out Value  // output result of evaluating an expression
	err error  // any error that occurs during evaluation (column or type mismatch)
}

func pStmt(p *Parser) (r interface{}) {
	switch {
	case pKeyword(p, "create", "table"):
		r = pCreateTable(p)
	case pKeyword(p, "select"):
		r = pSelect(p)
	}
	return r
}

func pSelect(p *Parser) *QLSelect {
	stmt := QLSelect{}
	pSelectExprList(p, &stmt)
	pExpect(p, "from", "expect `FROM` table")
	stmt.Table = pMustSym(p) // FROM table
	pScan(p, &stmt.QLScan)   // INDEX BY xx FILTER yy LIMIT zz
	return &stmt
}

func pSelectExprList(p *Parser, node *QLSelect) {
	pSelectExpr(p, node)
	for pKeyword(p, ",") {
		pSelectExpr(p, node)
	}
}
type qlScanIter struct {
	// input
	req *QLScan
	sc Scanner
	// state
	idx int64
	end bool
	// cached output item
	rec Record
	err

func pScan(p *Parser, node *QLScan) {
	if pKeyword(p, "index", "by") {
		pIndexBy(p, node)
	}
	if pKeyword(p, "filter") {
		pExprOr(p, &node.Filter)
	}
	node.Offset, node.Limit = 0, math.MaxInt64
	if pKeyword(p, "limit") {
		pLimit(p, node)
	}
}

// evalutes an expression tree rooted at node
func qlEval(ctx *QLEvalContex, node QLNode) {
	switch node.Type {
	// if the node is a column reference look into the env (example : age)
	case QL_SYM:
		if v := ctx.env.Get(string(node.Str)); v != nil {
			// if found set ctx.out to the value of that column
			ctx.out = *v
		} else {
			//  if not found ,it sets an error
			qlErr(ctx, "unknown column :%s", node.Str)
		}
	// QL_I64, QL_STR: Literal values
	case QL_I64, QL_STR:
		// sets the output to the literal value
		ctx.out = node.Value

	//  QL_NEG: Unary negation
	case QL_NEG:
		// it recursively evalutes the child node
		qlEval(ctx, node.Kids[0])
		//  if the result is an integer ,it negates it
		if ctx.out.Type == TYPE_INT64 {
			// this handle unary minus
			ctx.out.I64 = -ctx.out.I64
		} else {
			qlErr(ctx, "QL_NEG type error")
		}
	}

}

func qlScanInit(req *QLScan, sc *Scanner) (err error) {
	// evaluates the first key and stores key and vaue ,comparision operator
	//  eg : a>10
	if sc.Key1, sc.Cmp1, err = qlEvalScanKey(req.Key1); err != nil {
		return err
	}
	// evaluates the second key and stores key and vaue ,comparision operator
	//  eg : a<20
	if sc.Key2, sc.Cmp2, err = qlEvalScanKey(req.Key2); err != nil {
		return err
	}
	switch {
	// if both the keys are provided ,do full table scan
	case req.Key1.Type == 0 && req.Key2.Type == 0: // no `INDEX BY`
		sc.Cmp1, sc.Cmp2 = CMP_GE, CMP_LE // full table scan
	// Query like: WHERE a = 5 → treat it as range [5, 5] (so we do a >= 5 AND a <= 5).
	case req.Key1.Type == QL_CMP_EQ && req.Key2.Type == 0:
		sc.Key2 = sc.Key1
		sc.Cmp1, sc.Cmp2 = CMP_GE, CMP_LE

	// eg Query like: WHERE a > 10 → start from 10, go up.
	// Or WHERE a < 20 → start from beginning, go until 20.
	case req.Key1.Type != 0 && req.Key2.Type == 0:
		if sc.Cmp1 > 0 {
			sc.Cmp2 = CMP_LE // scan towards +∞
		} else {
			sc.Cmp2 = CMP_GE // scan towards -∞
		}

	}
	return nil
}
