import { Token, TokenType } from "./token";
import { Lexer } from "./lexer";


export class Tokens {
	private data: Token[];
	private maxPos: number;
	private lexer: Lexer;
	private skipInsignificant: boolean;

	constructor(text: string, maxQuerySize: number, skipInsignificant: boolean) {
		this.data = [];
		this.maxPos = 0;
		this.lexer = new Lexer(text, maxQuerySize);
		this.skipInsignificant = skipInsignificant;
	}

	public index(index: number) {
		while (true) {
			if (index < this.data.length) {
				this.maxPos = Math.max(this.maxPos, index);
				return this.data[index];
			}

			if (!this.isDataEmpty() && this.lastData().isEnd()) {
				this.maxPos = this.data.length - 1;
				return this.lastData();
			}

			const token = this.lexer.nextToken();

			if (!this.skipInsignificant || token.isSignificant()) {
				this.data.push(token);
			}
		}
	}

	public max(): Token {
		if (this.isDataEmpty()) {
			return null!;
		}

		return this.data[this.maxPos];
	}

	public reset() {
		this.maxPos = 0;
	}

	private isDataEmpty(): boolean {
		return this.data.length === 0
	}

	private lastData(): Token {
		return this.data[this.data.length - 1];
	}
}

export class TokenIterator {
	protected tokens: Tokens;
	public index: number;

	constructor(tokens: Tokens) {
		this.tokens = tokens;
		this.index = 0;
	}

	public incIndex() {
		this.index++;
	}

	public decIndex() {
		this.index--;
	}

	/**
	 * Return the token at the current index
	 */
	public get(): Token {
		return this.tokens.index(this.index);
	}

	public isValid(): boolean {
		return this.get().type < TokenType.EndOfStream;
	}

	/**
	 * Furthest token that was read
	 */
	public max(): Token {
		return this.tokens.max();
	}

	/**
	 * Mimic the "->" operator overload
	 **/

	// fix type script thinking type doesn't change
	public isType(type: TokenType): boolean {
		return this.type === type;
	}

	get type(): TokenType {
		return this.get().type;
	}

	get begin(): number {
		return this.get().begin;
	}

	get end(): number {
		return this.get().end;
	}

	// may be removed
	get text(): string {
		return this.get().text;
	}

	public size(): number {
		return this.get().size();
	}

	public isSignificant(): boolean {
		return this.get().isSignificant();
	}

	public isError(): boolean {
		return this.get().isError();
	}

	public isEnd(): boolean {
		return this.get().isEnd();
	}
}

/**
 * Token iterator augmented with depth information. This allows the control of recursion depth.
 */
export class Pos extends TokenIterator {
	private depth: number;
	private maxDepth: number;

	private backtracks: number;
	private maxBacktracks: number;

	constructor(tokens: Tokens, maxDepth: number, maxBacktracks: number) {
		super(tokens);
		this.depth = 0;
		this.maxDepth = maxDepth;
		this.backtracks = 0;
		this.maxBacktracks = maxBacktracks
	}

	public incDepth() {
		this.depth++;
		if (this.maxDepth > 0 && this.depth > this.maxDepth) {
			throw new Error('TOO_DEEP_RECURSION');
		}
	}
	
	public decDepth() {
		if (this.depth === 0) {
			throw new Error('incorrect calculation of parse depth');
		}

		this.depth--;
	}

	public clone(): Pos {
		const pos = new Pos(this.tokens, this.maxDepth, this.maxBacktracks);
		pos.index = this.index;
		pos.depth = this.depth;
		pos.backtracks = this.backtracks;
		
		return pos;
	}

	public assign(pos: Pos) {
		this.tokens = pos.tokens;
		this.index = pos.index;
		this.depth = pos.depth;
		this.maxDepth = pos.maxDepth;
		this.backtracks = pos.backtracks;
		this.maxBacktracks = pos.maxBacktracks;
	}
}