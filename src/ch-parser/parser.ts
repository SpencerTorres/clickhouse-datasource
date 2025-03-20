import { ASTPtr } from "./ast";
import { Pos } from "./tokenIterator";


export class Expected {
	private variants: string[];
	private maxParsedPos: number | null;

	constructor() {
		this.variants = [];
		this.maxParsedPos = null;
	}

	public add(currentPos: number, description: string) {
		if (!this.maxParsedPos || currentPos > this.maxParsedPos) {
			this.variants = [];
			this.maxParsedPos = currentPos;
			this.variants.push(description);

			return;
		}

		if ((currentPos === this.maxParsedPos) && (this.variants.indexOf(description) === this.variants.length)) {
			this.variants.push(description);
		}
	}
}

export abstract class Parser {
	constructor() {}

	public abstract getName(): string;

	public abstract parse(pos: Pos, node: ASTPtr, expected: Expected): boolean;

	public ignore(pos: Pos, expected?: Expected): boolean {
		const ignoreNode: ASTPtr = new ASTPtr(null);
		if (!expected) {
			expected = new Expected();
		}

		return this.parse(pos, ignoreNode, expected);		
	}

	/**
	 * The same as parse, but do not move the position and do not write the result to node
     */
	public check(pos: Pos, expected: Expected): boolean {
		const begin = pos.clone();
		const node = new ASTPtr();

		if (!this.parse(pos, node, expected)) {
			pos.assign(begin);
			return false;
		}

		return true;
	}

	/**
	 * The same as parse, but doesn't move the position even if parsing was successful.
     */
	public checkWithoutMoving(pos: Pos, expected: Expected): boolean {
		pos = pos.clone();
		const node = new ASTPtr();

		return this.parse(pos, node, expected);
	}
}
