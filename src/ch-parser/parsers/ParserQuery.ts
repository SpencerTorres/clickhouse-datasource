import { ASTExpressionList, ASTIdentifier, ASTPtr, ASTQueryParameter, ASTs, ASTSelectQuery, ASTSelectQueryExpression, ASTSelectWithUnionQuery, ASTWithElement } from "ch-parser/ast";
import { findFirstSymbols, readBackQuotedStringWithSQLStyle, readDoubleQuotedStringWithSQLStyle } from "ch-parser/helpers";
import { Keyword } from "ch-parser/keywords";
import { Expected, Parser } from "ch-parser/parser";
import { TokenType } from "ch-parser/token";
import { Pos } from "ch-parser/tokenIterator";


export class ParserToken extends Parser {
	private tokenType: TokenType;

	constructor(tokenType: TokenType) {
		super();
		this.tokenType = tokenType;
	}

	public getName(): string {
		return 'token';
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		if (pos.type !== this.tokenType) {
			// TODO: token names
			expected.add(pos.index, 'token');
			return false;
		}

		pos.incIndex();
		return true;
	}
}

export class ParserIdentifier extends Parser {
	private allowQueryParameter: boolean;

	constructor(allowQueryParameter: boolean = false) {
		super();
		this.allowQueryParameter = allowQueryParameter;
	}

	public getName(): string {
		return 'identifier';
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		// Identifier in backquotes or in double quotes or in English-style Unicode double quotes
		if (pos.isType(TokenType.QuotedIdentifier)) {
			// The case of Unicode quotes. No escaping is supported. Assuming UTF-8.
			// Empty identifiers are not allowed.
			if (pos.text[0] == '\xE2' && pos.size() > 6) {
				node.assign(new ASTIdentifier(pos.text.substring(3, pos.text.length - 3)));
				pos.incIndex();
				return true;
			}

			// Handle backquotes or double quotes
			let s: string = '';
			if (pos.text[0] === '`') {
				s = readBackQuotedStringWithSQLStyle(pos.text);
			} else {
				s = readDoubleQuotedStringWithSQLStyle(pos.text);
			}

			// Empty identifiers are not allowed
			if (s.length === 0) {
				return false;
			}

			node.assign(new ASTIdentifier(s));
			pos.incIndex();
			return true;
		}

		if (pos.isType(TokenType.BareWord)) {
			node.assign(new ASTIdentifier(pos.text));
			pos.incIndex();
			return true;
		}

		if (this.allowQueryParameter && pos.isType(TokenType.OpeningCurlyBrace)) {
			pos.incIndex();

			if (!pos.isType(TokenType.BareWord)) {
				expected.add(pos.index, "substitution name (identifier)");
				return false;
			}

			const name: string = pos.text;
			pos.incIndex();

			if (!pos.isType(TokenType.Colon)) {
				expected.add(pos.index, "colon between name and type");
				return false;
			}

			pos.incIndex();

			if (!pos.isType(TokenType.BareWord)) {
				expected.add(pos.index, "substitution type (identifier)");
				return false;
			}

			const type: string = pos.text;
			pos.incIndex();

			if (type !== "Identifier") {
				expected.add(pos.index, "substitution type (identifier)");
				return false;
			}

			if (!pos.isType(TokenType.ClosingCurlyBrace)) {
				expected.add(pos.index, "closing curly brace");
				return false;
			}

			pos.incIndex();
			node.assign(new ASTIdentifier("", new ASTPtr(new ASTQueryParameter(name, type))));
			return true;
		}

		return false;
	}
}


export class ParserKeyword extends Parser {
	private keyword: string;

	constructor(keyword: Keyword) {
		super();
		this.keyword = keyword;
	}

	public getName(): string {
		return this.keyword;
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		if (pos.type !== TokenType.BareWord) {
			return false;
		}

		let currentWord = this.keyword;

		while (true) {
			expected.add(pos.index, currentWord);

			if (pos.type !== TokenType.BareWord) {
				return false;
			}

			const nextWhitespace = findFirstSymbols(currentWord, 0, currentWord.length, ' ', '\0');
			const word = currentWord.substring(0, nextWhitespace);

			if (pos.size() !== word.length) {
				return false;
			}

			if (pos.text.toUpperCase() !== word.toUpperCase()) {
				return false;
			}

			pos.incIndex();

			if (nextWhitespace >= word.length) {
				break;
			}

			currentWord = currentWord.substring(nextWhitespace + 1);
		}

		return true;
	}
}

export class ParserAliasesExpressionList extends Parser {
	public getName(): string {
		return 'list of aliases expressions';
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		return new ParserList(new ParserIdentifier(), new ParserToken(TokenType.Comma), false).parse(pos, node, expected);
	}
}

export class ParserExpressionWithOptionalAlias extends Parser {
	private allowAliasWithoutAsKeyword: boolean;
	private isTableFunction: boolean;
	private allowTrailingCommas: boolean;

	constructor(allowAliasWithoutAsKeyword: boolean, isTableFunction: boolean = false, allowTrailingCommas: boolean = false) {
		super();
		this.allowAliasWithoutAsKeyword = allowAliasWithoutAsKeyword;
		this.isTableFunction = isTableFunction;
		this.allowTrailingCommas = allowTrailingCommas;
	}

	public getName(): string {
		return 'expression with optional alias';
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		// TODO: continue
		return new ParserList(new ParserIdentifier(), new ParserToken(TokenType.Comma), false).parse(pos, node, expected);
	}
}

export class ParserWithElement extends Parser {
	public getName(): string {
		return 'WITH element';
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
        const s_ident = new ParserIdentifier();
        const s_as = new ParserKeyword(Keyword.AS);
        const s_subquery = new ParserSubquery();
        const exp_list_for_aliases = new ParserAliasesExpressionList();
        const open_bracket = new ParserToken(TokenType.OpeningRoundBracket);
        const close_bracket = new ParserToken(TokenType.ClosingRoundBracket);
        
        const old_pos = pos.clone();
        const with_element = new ASTWithElement();
        
        // Trying to parse structure: identifier [(alias1, alias2, ...)] AS (subquery)
        const name_or_expr = new ASTPtr();
        
        // Parse identifier or expression
        if (!(s_ident.parse(pos, name_or_expr, expected) || 
              new ParserExpressionWithOptionalAlias(false).parse(pos, name_or_expr, expected))) {
            pos.assign(old_pos);
            
            // If we can't parse the first structure, try just a simple expression with optional alias
            const s_expr = new ParserExpressionWithOptionalAlias(false);
            return s_expr.parse(pos, node, expected);
        }
        
        // Parse optional aliases list in brackets
        const parse_aliases = (): boolean => {
            const saved_pos = pos.clone();
            if (open_bracket.ignore(pos, expected)) {
                const expression_list_for_aliases = new ASTPtr();
                if (exp_list_for_aliases.parse(pos, expression_list_for_aliases, expected)) {
                    with_element.aliases = expression_list_for_aliases;
                    if (!close_bracket.ignore(pos, expected)) {
                        return false;
                    }
                    return true;
                } else {
                    pos.assign(saved_pos);
                    return false;
                }
            }
            return true;
        };
        
        // Check for aliases and AS and subquery
        if (!parse_aliases() || 
            !s_as.ignore(pos, expected) || 
            !s_subquery.parse(pos, with_element.subquery, expected)) {
            pos.assign(old_pos);
            
            // If we can't parse the complete structure, try just a simple expression with optional alias
            const s_expr = new ParserExpressionWithOptionalAlias(false);
            return s_expr.parse(pos, node, expected);
        }
        
        // Extract name if possible
        if (name_or_expr.ptr) {
            this.tryGetIdentifierNameInto(name_or_expr, with_element);
        }
        
        // Add subquery to children
        with_element.children = [with_element.subquery];
        node.assign(with_element);
        
        return true;
    }
    
    private tryGetIdentifierNameInto(node: ASTPtr, nameRef: { name: string }): void {
        if (node.ptr instanceof ASTIdentifier) {
            nameRef.name = node.ptr.getName();
        }
    }
}

export class ParserSelectWithUnionQuery extends Parser {
	public getName(): string {
		return 'SELECT query, possibly with UNION';
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		const listNode = new ASTPtr();
		const parser = new ParserUnionList();

		if (!parser.parse(pos, listNode, expected)) {
			return false;
		}

		const selectWithUnionQuery = new ASTSelectWithUnionQuery();
		selectWithUnionQuery.children = [listNode];
		node.assign(selectWithUnionQuery);

		return true;
	}
}

export class ParserList extends Parser {
	private elementParser: Parser;
	private separatorParser: Parser;
	private allowEmpty: boolean;
	private resultSeparator: string;

	constructor(elementParser: Parser, separatorParser: Parser, allowEmpty: boolean = true, resultSeparator: string = ',') {
		super();
		this.elementParser = elementParser;
		this.separatorParser = separatorParser;
		this.allowEmpty = allowEmpty;
		this.resultSeparator = resultSeparator;
	}

	public getName(): string {
		return 'list of elements';
	}

	public parseUtil(pos: Pos, parseElement: (pos: Pos) => boolean, parseSeparator: (pos: Pos) => boolean, allowEmpty: boolean): boolean {
		let begin = pos.clone();
		if (!parseElement(pos)) {
			pos.assign(begin);
			return allowEmpty;
		}

		while (true) {
			begin = pos.clone();
			if (!parseSeparator(pos) || !parseSeparator(pos)) {
				pos.assign(begin);
				return true;
			}
		}
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		const elementParser = this.elementParser;
		const separatorParser = this.separatorParser;
		const elements: ASTs = [];

		function parseElement(pos: Pos): boolean {
			const element = new ASTPtr();
			if (!elementParser.parse(pos, element, expected)) {
				return false;
			}

			elements.push(element);
			return true;
		}

		function parseSeparator(pos: Pos): boolean {
			return separatorParser.ignore(pos, expected);
		}

		if (!this.parseUtil(pos, parseElement, parseSeparator, this.allowEmpty)) {
			return false;
		}

		const expressionList = new ASTExpressionList(this.resultSeparator);
		expressionList.children = elements;
		node.assign(expressionList);

		return true;
	}
}

export class ParserUnionList extends Parser {
	public getName(): string {
		return 'list of union elements';
	}

	public parseUtil(pos: Pos, parseElement: (pos: Pos) => boolean, parseSeparator: (pos: Pos) => boolean): boolean {
		let begin = pos.clone();
		if (!parseElement(pos)) {
			pos.assign(begin);
			return false;
		}

		while (true) {
			begin = pos.clone();
			if (!parseSeparator(pos) || !parseSeparator(pos)) {
				pos.assign(begin);
				return true;
			}
		}
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		const elemParser = new ParserUnionQueryElement();
		const elements: ASTs = [];

		function parseElement(pos: Pos): boolean {
			const element = new ASTPtr();
			if (!elemParser.parse(pos, element, expected)) {
				return false;
			}

			elements.push(element);
			return true;
		}

		function parseSeparator(pos: Pos): boolean {
			return false;
		}

		if (!this.parseUtil(pos, parseElement, parseSeparator)) {
			return false;
		}

		const expressionList = new ASTExpressionList();
		expressionList.children = elements;
		node.assign(expressionList);

		return true;
	}
}

export class ParserUnionQueryElement extends Parser {
	public getName(): string {
		return 'SELECT query, subquery, possibly with UNION';
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		if (!(new ParserSubquery()).parse(pos, node, expected) && !(new ParserSelectQuery()).parse(pos, node, expected)) {
			return false;
		}

		return true;
	}
}

export class ParserSubquery extends Parser {
	public getName(): string {
		return 'SELECT subquery';
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
		const select = new ParserSelectWithUnionQuery();

		if (!pos.isType(TokenType.OpeningRoundBracket)) {
			return false;
		}
		pos.incIndex();

		const resultNode = new ASTPtr();

		const selectNode = new ASTPtr();
		if (select.parse(pos, selectNode, expected)) {
			resultNode.assign(selectNode.ptr)
		} else {
			return false;
		}

		if (!pos.isType(TokenType.ClosingRoundBracket)) {
			return false;
		}
		pos.incIndex();

		node.assign(resultNode.ptr);
		return true;
	}
}

export class ParserSelectQuery extends Parser {
	public getName(): string {
		return 'SELECT query';
	}

	public parse(pos: Pos, node: ASTPtr, expected: Expected): boolean {
        const selectQuery = new ASTSelectQuery();
        node.assign(selectQuery);

        // Define keywords
        const sSelect = new ParserKeyword(Keyword.SELECT);
        const sFrom = new ParserKeyword(Keyword.FROM);
        const sWhere = new ParserKeyword(Keyword.WHERE);
        const sGroupBy = new ParserKeyword(Keyword.GROUP_BY);
        const sWith = new ParserKeyword(Keyword.WITH);
        const sOrderBy = new ParserKeyword(Keyword.ORDER_BY);
        const sLimit = new ParserKeyword(Keyword.LIMIT);
        const sSettings = new ParserKeyword(Keyword.SETTINGS);

        // Define parsers
        const expElem = new ParserExpressionWithOptionalAlias(false);
        
        // Define token parsers
        const openBracket = new ParserToken(TokenType.OpeningRoundBracket);
        const closeBracket = new ParserToken(TokenType.ClosingRoundBracket);

        // Define AST nodes to hold parsed elements
        const withExpressionList = new ASTPtr();
        const selectExpressionList = new ASTPtr();
        const tables = new ASTPtr();
        const whereExpression = new ASTPtr();
        const groupExpressionList = new ASTPtr();
        const orderExpressionList = new ASTPtr();
        const limitLength = new ASTPtr();
        const limitOffset = new ASTPtr();
        const settings = new ASTPtr();

        // Parse WITH clause
        // if (!sWith.ignore(pos, expected)) {
        //     const withParser = new ParserList(new ParserWithElement(), new ParserToken(TokenType.Comma));
        //     if (!withParser.parse(pos, withExpressionList, expected)) {
        //         return false;
        //     }
        // }

        // Parse FROM clause first (optional)
        if (sFrom.ignore(pos, expected)) {
            // TODO: We're simplifying here since ParserTablesInSelectQuery isn't defined yet
            if (!expElem.parse(pos, tables, expected)) {
                return false;
            }
        }

        // Parse SELECT clause
        if (!sSelect.ignore(pos, expected)) {
            return false;
        }

        // Parse select expression list
        if (!expElem.parse(pos, selectExpressionList, expected)) {
            return false;
        }

        // Parse WHERE clause
        if (sWhere.ignore(pos, expected)) {
            if (!expElem.parse(pos, whereExpression, expected)) {
                return false;
            }
        }

        // Parse GROUP BY clause
        if (sGroupBy.ignore(pos, expected)) {
            if (!expElem.parse(pos, groupExpressionList, expected)) {
                return false;
            }
        }

        // Parse ORDER BY clause
        if (sOrderBy.ignore(pos, expected)) {
            if (!expElem.parse(pos, orderExpressionList, expected)) {
                return false;
            }
        }

        // Parse LIMIT clause
        if (sLimit.ignore(pos, expected)) {
            if (!expElem.parse(pos, limitLength, expected)) {
                return false;
            }

            // Check for offset in form LIMIT offset, length
            const comma = new ParserToken(TokenType.Comma);
            if (comma.ignore(pos, expected)) {
                limitOffset.assign(limitLength.ptr);
                if (!expElem.parse(pos, limitLength, expected)) {
                    return false;
                }
            }
        }

        // Parse SETTINGS
        if (sSettings.ignore(pos, expected)) {
            if (!expElem.parse(pos, settings, expected)) {
                return false;
            }
        }

        // Set all expressions to the select query
        if (withExpressionList.ptr) {
            selectQuery.setExpression(ASTSelectQueryExpression.WITH, withExpressionList);
        }
        
        if (selectExpressionList.ptr) {
            selectQuery.setExpression(ASTSelectQueryExpression.SELECT, selectExpressionList);
        }
        
        if (tables.ptr) {
            selectQuery.setExpression(ASTSelectQueryExpression.TABLES, tables);
        }
        
        if (whereExpression.ptr) {
            selectQuery.setExpression(ASTSelectQueryExpression.WHERE, whereExpression);
        }
        
        if (groupExpressionList.ptr) {
            selectQuery.setExpression(ASTSelectQueryExpression.GROUP_BY, groupExpressionList);
        }
        
        if (orderExpressionList.ptr) {
            selectQuery.setExpression(ASTSelectQueryExpression.ORDER_BY, orderExpressionList);
        }
        
        if (limitLength.ptr) {
            selectQuery.setExpression(ASTSelectQueryExpression.LIMIT_LENGTH, limitLength);
        }
        
        if (limitOffset.ptr) {
            selectQuery.setExpression(ASTSelectQueryExpression.LIMIT_OFFSET, limitOffset);
        }
        
        if (settings.ptr) {
            selectQuery.setExpression(ASTSelectQueryExpression.SETTINGS, settings);
        }

        return true;
    }
}