// DynamoDB expression engine: ConditionExpression / FilterExpression / KeyConditionExpression,
// UpdateExpression (SET / REMOVE / ADD / DELETE) and ProjectionExpression.
import { AwsError } from '../lib/common.mjs';

// ------------------------------------------------------------------ attribute values
export function cmpVal(a, b) {
  if (!a || !b) return NaN;
  if ('N' in a && 'N' in b) return Number(a.N) - Number(b.N);
  if ('S' in a && 'S' in b) return a.S < b.S ? -1 : a.S > b.S ? 1 : 0;
  if ('B' in a && 'B' in b) return Buffer.compare(Buffer.from(a.B, 'base64'), Buffer.from(b.B, 'base64'));
  return NaN;
}
export function eqVal(a, b) {
  if (!a || !b) return false;
  return JSON.stringify(canon(a)) === JSON.stringify(canon(b));
}
function canon(v) {
  if ('N' in v) return { N: String(Number(v.N)) };
  if ('NS' in v) return { NS: [...v.NS].map((n) => String(Number(n))).sort() };
  if ('SS' in v) return { SS: [...v.SS].sort() };
  if ('M' in v) return { M: Object.fromEntries(Object.keys(v.M).sort().map((k) => [k, canon(v.M[k])])) };
  if ('L' in v) return { L: v.L.map(canon) };
  return v;
}

// ------------------------------------------------------------------ tokenizer / parser
const TOKEN = /\s*(<>|<=|>=|=|<|>|\(|\)|,|\[|\]|\.|\+|-|:[\w]+|#[\w]+|[A-Za-z_][\w]*|\d+)/y;

export function tokenize(src) {
  const out = [];
  TOKEN.lastIndex = 0;
  let m;
  const s = String(src || '');
  while (TOKEN.lastIndex < s.length && (m = TOKEN.exec(s))) out.push(m[1]);
  if (s.slice(TOKEN.lastIndex).trim()) throw new AwsError('ValidationException', `Invalid expression near: ${s.slice(TOKEN.lastIndex, TOKEN.lastIndex + 20)}`);
  return out;
}

class Parser {
  constructor(src, names = {}, values = {}) {
    this.t = tokenize(src);
    this.i = 0;
    this.names = names;
    this.values = values;
  }
  peek(k = 0) {
    return this.t[this.i + k];
  }
  next() {
    return this.t[this.i++];
  }
  eat(tok) {
    if ((this.peek() || '').toUpperCase() !== tok) throw new AwsError('ValidationException', `Invalid expression: expected ${tok} but found ${this.peek() ?? 'end'}`);
    return this.next();
  }
  isKw(k) {
    return (this.peek() || '').toUpperCase() === k;
  }
  // path: name ( '.' name | '[' n ']' )*
  path() {
    const parts = [];
    const name = () => {
      const tk = this.next();
      if (!tk) throw new AwsError('ValidationException', 'Invalid expression: unexpected end');
      if (tk.startsWith('#')) {
        if (!(tk in this.names)) throw new AwsError('ValidationException', `Value provided in ExpressionAttributeNames unused or missing: ${tk}`);
        return this.names[tk];
      }
      return tk;
    };
    parts.push(name());
    for (;;) {
      if (this.peek() === '.') {
        this.next();
        parts.push(name());
      } else if (this.peek() === '[') {
        this.next();
        parts.push(Number(this.next()));
        this.eat(']');
      } else break;
    }
    return { path: parts };
  }
  operand() {
    const tk = this.peek();
    if (tk?.startsWith(':')) {
      this.next();
      if (!(tk in this.values)) throw new AwsError('ValidationException', `An expression attribute value used in expression is not defined; attribute value: ${tk}`);
      return { value: this.values[tk] };
    }
    if (/^(size|if_not_exists|list_append)$/i.test(tk || '') && this.peek(1) === '(') {
      const fn = this.next().toLowerCase();
      this.next();
      const args = [this.operand()];
      while (this.peek() === ',') {
        this.next();
        args.push(this.operand());
      }
      this.eat(')');
      return { fn, args };
    }
    return this.path();
  }
  // condition grammar
  cond() {
    let left = this.and();
    while (this.isKw('OR')) {
      this.next();
      left = { op: 'OR', a: left, b: this.and() };
    }
    return left;
  }
  and() {
    let left = this.not();
    while (this.isKw('AND')) {
      this.next();
      left = { op: 'AND', a: left, b: this.not() };
    }
    return left;
  }
  not() {
    if (this.isKw('NOT')) {
      this.next();
      return { op: 'NOT', a: this.not() };
    }
    return this.atom();
  }
  atom() {
    if (this.peek() === '(') {
      this.next();
      const c = this.cond();
      this.eat(')');
      return c;
    }
    const tk = (this.peek() || '').toLowerCase();
    if (['attribute_exists', 'attribute_not_exists', 'begins_with', 'contains', 'attribute_type'].includes(tk) && this.peek(1) === '(') {
      this.next();
      this.next();
      const args = [this.operand()];
      while (this.peek() === ',') {
        this.next();
        args.push(this.operand());
      }
      this.eat(')');
      return { op: tk, args };
    }
    const a = this.operand();
    const op = (this.peek() || '').toUpperCase();
    if (['=', '<>', '<', '<=', '>', '>='].includes(op)) {
      this.next();
      return { op, a, b: this.operand() };
    }
    if (op === 'BETWEEN') {
      this.next();
      const lo = this.operand();
      this.eat('AND');
      return { op: 'BETWEEN', a, lo, hi: this.operand() };
    }
    if (op === 'IN') {
      this.next();
      this.eat('(');
      const list = [this.operand()];
      while (this.peek() === ',') {
        this.next();
        list.push(this.operand());
      }
      this.eat(')');
      return { op: 'IN', a, list };
    }
    throw new AwsError('ValidationException', `Invalid expression: unexpected token ${this.peek() ?? 'end'}`);
  }
  done() {
    if (this.i < this.t.length) throw new AwsError('ValidationException', `Invalid expression: unexpected token ${this.peek()}`);
  }
}

export function getPath(item, path) {
  let cur = { M: item };
  for (const p of path) {
    if (!cur) return undefined;
    if (typeof p === 'number') cur = cur.L?.[p];
    else cur = cur.M?.[p];
  }
  return cur;
}

function setPath(item, path, val) {
  let cur = { M: item };
  for (let i = 0; i < path.length - 1; i++) {
    const p = path[i];
    const nxt = typeof p === 'number' ? cur.L?.[p] : cur.M?.[p];
    if (!nxt) throw new AwsError('ValidationException', 'The document path provided in the update expression is invalid for update');
    cur = nxt;
  }
  const last = path.at(-1);
  if (typeof last === 'number') {
    if (!cur.L) throw new AwsError('ValidationException', 'The document path provided in the update expression is invalid for update');
    if (last >= cur.L.length) cur.L.push(val);
    else cur.L[last] = val;
  } else {
    if (!cur.M) throw new AwsError('ValidationException', 'The document path provided in the update expression is invalid for update');
    cur.M[last] = val;
  }
}

function removePath(item, path) {
  let cur = { M: item };
  for (let i = 0; i < path.length - 1; i++) cur = typeof path[i] === 'number' ? cur?.L?.[path[i]] : cur?.M?.[path[i]];
  if (!cur) return;
  const last = path.at(-1);
  if (typeof last === 'number') cur.L?.splice(last, 1);
  else if (cur.M) delete cur.M[last];
}

function evalOperand(o, item) {
  if (o.value) return o.value;
  if (o.path) return getPath(item, o.path);
  if (o.fn === 'size') {
    const v = evalOperand(o.args[0], item);
    if (!v) return undefined;
    const n = v.S != null ? v.S.length : v.B != null ? Buffer.from(v.B, 'base64').length : v.L ? v.L.length : v.M ? Object.keys(v.M).length : (v.SS || v.NS || v.BS || []).length;
    return { N: String(n) };
  }
  if (o.fn === 'if_not_exists') return evalOperand(o.args[0], item) ?? evalOperand(o.args[1], item);
  if (o.fn === 'list_append') {
    const a = evalOperand(o.args[0], item);
    const b = evalOperand(o.args[1], item);
    return { L: [...(a?.L || []), ...(b?.L || [])] };
  }
  throw new AwsError('ValidationException', 'Unsupported operand');
}

function typeOf(v) {
  return v ? Object.keys(v)[0] : undefined;
}

function evalCond(n, item) {
  switch (n.op) {
    case 'OR':
      return evalCond(n.a, item) || evalCond(n.b, item);
    case 'AND':
      return evalCond(n.a, item) && evalCond(n.b, item);
    case 'NOT':
      return !evalCond(n.a, item);
    case 'attribute_exists':
      return evalOperand(n.args[0], item) !== undefined;
    case 'attribute_not_exists':
      return evalOperand(n.args[0], item) === undefined;
    case 'attribute_type':
      return typeOf(evalOperand(n.args[0], item)) === evalOperand(n.args[1], item)?.S;
    case 'begins_with': {
      const v = evalOperand(n.args[0], item);
      const p = evalOperand(n.args[1], item);
      return !!v && !!p && (v.S != null ? v.S.startsWith(p.S) : v.B != null ? Buffer.from(v.B, 'base64').toString('latin1').startsWith(Buffer.from(p.B, 'base64').toString('latin1')) : false);
    }
    case 'contains': {
      const v = evalOperand(n.args[0], item);
      const x = evalOperand(n.args[1], item);
      if (!v || !x) return false;
      if (v.S != null) return x.S != null && v.S.includes(x.S);
      if (v.SS) return v.SS.includes(x.S);
      if (v.NS) return v.NS.map(Number).includes(Number(x.N));
      if (v.L) return v.L.some((e) => eqVal(e, x));
      return false;
    }
    case '=':
      return eqVal(evalOperand(n.a, item), evalOperand(n.b, item));
    case '<>': {
      const a = evalOperand(n.a, item);
      const b = evalOperand(n.b, item);
      return !(a && b && eqVal(a, b));
    }
    case '<':
      return cmpVal(evalOperand(n.a, item), evalOperand(n.b, item)) < 0;
    case '<=':
      return cmpVal(evalOperand(n.a, item), evalOperand(n.b, item)) <= 0;
    case '>':
      return cmpVal(evalOperand(n.a, item), evalOperand(n.b, item)) > 0;
    case '>=':
      return cmpVal(evalOperand(n.a, item), evalOperand(n.b, item)) >= 0;
    case 'BETWEEN': {
      const v = evalOperand(n.a, item);
      return cmpVal(v, evalOperand(n.lo, item)) >= 0 && cmpVal(v, evalOperand(n.hi, item)) <= 0;
    }
    case 'IN': {
      const v = evalOperand(n.a, item);
      return n.list.some((o) => eqVal(v, evalOperand(o, item)));
    }
  }
  throw new AwsError('ValidationException', 'Unsupported condition');
}

export function compileCondition(expr, names, values) {
  if (!expr) return () => true;
  const p = new Parser(expr, names, values);
  const ast = p.cond();
  p.done();
  return (item) => evalCond(ast, item || {});
}

/** For Query: split KeyConditionExpression into partition-key equality + optional sort condition. */
export function parseKeyCondition(expr, names, values) {
  const p = new Parser(expr, names, values);
  const ast = p.cond();
  p.done();
  const parts = [];
  const walk = (n) => (n.op === 'AND' ? (walk(n.a), walk(n.b)) : parts.push(n));
  walk(ast);
  return { parts, test: (item) => evalCond(ast, item) };
}

// ------------------------------------------------------------------ UpdateExpression
export function applyUpdate(item, expr, names = {}, values = {}) {
  const p = new Parser(expr, names, values);
  const clauses = { SET: [], REMOVE: [], ADD: [], DELETE: [] };
  while (p.peek()) {
    const kw = p.next().toUpperCase();
    if (!(kw in clauses)) throw new AwsError('ValidationException', `Invalid UpdateExpression: unexpected ${kw}`);
    for (;;) {
      if (kw === 'SET') {
        const target = p.path();
        p.eat('=');
        let val = p.operand();
        if (p.peek() === '+' || p.peek() === '-') {
          const op = p.next();
          val = { arith: op, a: val, b: p.operand() };
        }
        clauses.SET.push({ target, val });
      } else if (kw === 'REMOVE') clauses.REMOVE.push(p.path());
      else clauses[kw].push({ target: p.path(), val: p.operand() });
      if (p.peek() === ',') p.next();
      else break;
    }
  }
  const snapshot = JSON.parse(JSON.stringify(item));
  const ev = (o) => {
    if (o.arith) {
      const a = evalOperand(o.a, snapshot);
      const b = evalOperand(o.b, snapshot);
      if (a?.N == null || b?.N == null) throw new AwsError('ValidationException', 'An operand in the update expression has an incorrect data type');
      return { N: String(o.arith === '+' ? Number(a.N) + Number(b.N) : Number(a.N) - Number(b.N)) };
    }
    return evalOperand(o, snapshot);
  };
  for (const { target, val } of clauses.SET) {
    const v = ev(val);
    if (v === undefined) throw new AwsError('ValidationException', 'The provided expression refers to an attribute that does not exist in the item');
    setPath(item, target.path, JSON.parse(JSON.stringify(v)));
  }
  for (const t of clauses.REMOVE) removePath(item, t.path);
  for (const { target, val } of clauses.ADD) {
    const cur = getPath(item, target.path);
    const v = evalOperand(val, snapshot);
    if (v.N != null) setPath(item, target.path, { N: String(Number(cur?.N || 0) + Number(v.N)) });
    else {
      const k = typeOf(v);
      setPath(item, target.path, { [k]: [...new Set([...(cur?.[k] || []), ...v[k]])] });
    }
  }
  for (const { target, val } of clauses.DELETE) {
    const cur = getPath(item, target.path);
    const v = evalOperand(val, snapshot);
    const k = typeOf(v);
    if (cur?.[k]) {
      const left = cur[k].filter((x) => !v[k].includes(x));
      if (left.length) setPath(item, target.path, { [k]: left });
      else removePath(item, target.path);
    }
  }
  return item;
}

export function project(item, expr, names = {}) {
  if (!expr) return item;
  const out = {};
  for (const raw of expr.split(',')) {
    const p = new Parser(raw.trim(), names, {});
    const { path } = p.path();
    const v = getPath(item, path);
    if (v !== undefined) {
      if (path.length === 1) out[path[0]] = v;
      else {
        // rebuild nested maps for nested projections
        let cur = out;
        for (let i = 0; i < path.length - 1; i++) {
          const k = path[i];
          cur[k] ||= { M: {} };
          cur = cur[k].M;
        }
        cur[path.at(-1)] = v;
      }
    }
  }
  return out;
}
