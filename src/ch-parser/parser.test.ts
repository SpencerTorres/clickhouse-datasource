import { ASTPtr, IAST } from "./ast";
import { Expected, Parser } from "./parser";
import { ParserSelectWithUnionQuery } from "./parsers/ParserQuery";
import { TokenType } from "./token";
import { Pos, Tokens } from "./tokenIterator";

function tryParseQuery(parser: Parser, text: string, maxQuerySize: number, maxParserDepth: number, maxParserBacktracks: number, skipInsignificant: boolean): ASTPtr {
	const tokens = new Tokens(text, maxQuerySize, skipInsignificant);
	const tokenIterator = new Pos(tokens, maxParserDepth, maxParserBacktracks);


	if (tokenIterator.isEnd() || tokenIterator.begin == TokenType.Semicolon) {
		// Empty query
		return new ASTPtr();
	}

	const expected = new Expected();

	const result = new ASTPtr();
	const parserResult = parser.parse(tokenIterator, result, expected);

	if (!parserResult) {
		console.error('parsing failed', JSON.stringify(expected));
	}

	return result;
}

test('test parsing', () => {
	const goodSql = `
		SELECT
			a,
			b,
			c
		FROM
			alphabet.letters
		WHERE
			a > b
		ORDER BY b ASC
		LIMIT 1
	`;

	const sql = `
		(SELECT "test" FROM) (SELECT "test" FROM) (SELECT FROM)
	`;

	const maxQuerySize = 10000;
	const maxParserDepth = 500;
	const maxParserBacktracks = 500;
	const skipInsignificant = true;

	const parser = new ParserSelectWithUnionQuery();
	const astPtr = tryParseQuery(parser, sql, maxQuerySize, maxParserDepth, maxParserBacktracks, skipInsignificant);
	console.log(JSON.stringify(astPtr.ptr, undefined, '\t'));

	return;
});