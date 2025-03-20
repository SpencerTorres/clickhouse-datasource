
export class IAST {
	public children: ASTs;

	constructor() {
		this.children = [];
	}
}

export class ASTExpressionList extends IAST {
	public separator: string;

	constructor(separator: string = ',') {
		super();
		this.separator = separator;
	}
}

export class ASTSelectWithUnionQuery extends IAST { }

export enum ASTSelectQueryExpression {
	WITH,
	SELECT,
	TABLES,
	ALIASES,
	CTE_ALIASES,
	PREWHERE,
	WHERE,
	GROUP_BY,
	HAVING,
	WINDOW,
	QUALIFY,
	ORDER_BY,
	LIMIT_BY_OFFSET,
	LIMIT_BY_LENGTH,
	LIMIT_BY,
	LIMIT_OFFSET,
	LIMIT_LENGTH,
	SETTINGS,
	INTERPOLATE
};

export class ASTSelectQuery extends IAST {
	private expressions = new Map<ASTSelectQueryExpression, ASTPtr>();

	public getExpression(expression: ASTSelectQueryExpression): ASTPtr {
		return this.expressions.get(expression) || new ASTPtr();
	}

	public setExpression(expression: ASTSelectQueryExpression, ptr: ASTPtr) {
		this.expressions.set(expression, ptr);
	}
}

export class ASTWithElement extends IAST {
	public name: string = '';
	public subquery: ASTPtr = new ASTPtr();
	public aliases: ASTPtr = new ASTPtr();
}

export class ASTWithAlias extends IAST {
	protected alias: string = '';
	protected preferAliasToColumnName: boolean = false;
	protected parameterizedAlias: ASTPtr = new ASTPtr();

	public setAlias(alias: string) {
		this.alias = alias;
	}
}

export class ASTQueryParameter extends ASTWithAlias {
	private name: string;
	private type: string;

	constructor(name: string, type: string) {
		super();
		this.name = name;
		this.type = type;
	}
}

export class ASTIdentifier extends ASTWithAlias {
	private fullName: string;

	constructor(fullName: string, nameParam?: ASTPtr) {
		super();
		this.fullName = fullName;
		if (nameParam) {
			this.parameterizedAlias = nameParam;
		}
	}

	public getName(): string {
		return this.fullName;
	}
}

export class ASTPtr {
	public ptr: IAST | null

	constructor(ptr: IAST | null = null) {
		this.ptr = null;
	}

	public assign(ptr: IAST | null) {
		this.ptr = ptr;
	}
};

export type ASTs = ASTPtr[];
