/**
 * Helper functions for character classification and string handling
 */

/**
 * Check if a character is a whitespace ASCII character
 */
export function isWhitespaceASCII(c: string): boolean {
	return c === ' ' || c === '\t' || c === '\n' || c === '\r' || c === '\f' || c === '\v';
}

/**
 * Check if a character is a numeric ASCII character
 */
export function isNumericASCII(c: string): boolean {
	return c >= '0' && c <= '9';
}

/**
 * Check if a character is a word character (letter, digit, or underscore)
 */
export function isWordCharASCII(c: string): boolean {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c === '_';
}

/**
 * Check if a character is a hexadecimal digit
 */
export function isHexDigit(c: string): boolean {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

/**
 * Check if a character is a valid number separator, like underscore in 1_000_000
 */
export function isNumberSeparator(startOfBlock: boolean, hex: boolean, pos: number, text: string): boolean {
	if (startOfBlock) {
		return false;
	}

	if (pos >= text.length) {
		return false;
	}

	if (text[pos] !== '_') {
		return false;
	}

	if (pos + 1 >= text.length) {
		return false;
	}

	if (hex) {
		return isHexDigit(text[pos + 1]);
	}

	return isNumericASCII(text[pos + 1]);
}

/**
 * Find the first occurrence of any of the given characters
 */
export function findFirstSymbols(text: string, pos: number, end: number, ...symbols: string[]): number {
	while (pos < end) {
		if (symbols.includes(text[pos])) {
			return pos;
		}
		pos++;
	}
	return end;
}

/**
 * Find the first character that is not any of the given characters
 */
export function findFirstNotSymbols(text: string, pos: number, end: number, ...symbols: string[]): number {
	while (pos < end) {
		if (!symbols.includes(text[pos])) {
			return pos;
		}
		pos++;
	}
	return end;
}

/**
 * Skip UTF-8 whitespaces (including Unicode ones)
 */
export function skipWhitespacesUTF8(text: string, pos: number, end: number): number {
	// Skip whitespace characters in Unicode
	// This is a simplified version that just skips common Unicode whitespace
	while (pos < end) {
		const code = text.charCodeAt(pos);

		// Skip ASCII whitespace
		if (code <= 127 && isWhitespaceASCII(String.fromCharCode(code))) {
			pos++;
			continue;
		}

		// Skip some common Unicode whitespace
		// U+00A0 - NO-BREAK SPACE
		// U+2000 to U+200A - Various space characters
		// U+2028 - LINE SEPARATOR
		// U+2029 - PARAGRAPH SEPARATOR
		// U+202F - NARROW NO-BREAK SPACE
		// U+205F - MEDIUM MATHEMATICAL SPACE
		// U+3000 - IDEOGRAPHIC SPACE
		if (
			code === 0x00A0 ||
			(code >= 0x2000 && code <= 0x200A) ||
			code === 0x2028 ||
			code === 0x2029 ||
			code === 0x202F ||
			code === 0x205F ||
			code === 0x3000
		) {
			pos++;
			continue;
		}

		break;
	}

	return pos;
}

/**
 * Check if a character is a UTF-8 continuation octet
 */
export function isContinuationOctet(c: string): boolean {
	const code = c.charCodeAt(0);
	return (code & 0xC0) === 0x80;
}

/**
 * Reads a quoted string with SQL style quoting
 * In SQL style, quotes within the string are escaped by doubling them
 * e.g., 'It''s a test' is parsed as "It's a test"
 */
export function readAnyQuotedString(text: string, quote: string, enableSqlStyleQuoting: boolean): string {
    if (!text || text.length === 0 || text[0] !== quote) {
        throw new Error(`Cannot parse quoted string: expected opening quote '${quote}', got '${text[0] || "EOF"}'`);
    }

    let result = '';
    let pos = 1; // Skip the opening quote

    while (pos < text.length) {
        // Find the next escape character or quote
        const nextEscapeOrQuote = text.indexOf(quote, pos);
        const nextBackslash = text.indexOf('\\', pos);
        
        let nextSpecialChar = -1;
        if (nextEscapeOrQuote !== -1 && (nextBackslash === -1 || nextEscapeOrQuote < nextBackslash)) {
            nextSpecialChar = nextEscapeOrQuote;
        } else if (nextBackslash !== -1) {
            nextSpecialChar = nextBackslash;
        }

        if (nextSpecialChar === -1) {
            // No more special characters, but we're missing the closing quote
            throw new Error("Cannot parse quoted string: expected closing quote");
        }

        // Append everything up to the special character
        result += text.substring(pos, nextSpecialChar);
        pos = nextSpecialChar + 1;

        if (pos >= text.length) {
            throw new Error("Cannot parse quoted string: unexpected end of string");
        }

        if (text[nextSpecialChar] === quote) {
            // Check for SQL style quoting (double quotes)
            if (enableSqlStyleQuoting && pos < text.length && text[pos] === quote) {
                result += quote;
                pos++;
            } else {
                // This is the closing quote
                return result;
            }
        } else if (text[nextSpecialChar] === '\\') {
            // Handle escape sequences
            if (pos < text.length) {
                const escaped = this.parseEscapeSequence(text, pos);
                result += escaped.char;
                pos = escaped.newPos;
            } else {
                throw new Error("Cannot parse quoted string: unexpected end of escape sequence");
            }
        }
    }

    throw new Error("Cannot parse quoted string: expected closing quote");
}

/**
 * Parses an escape sequence starting at the given position
 */
export function parseEscapeSequence(text: string, pos: number): { char: string, newPos: number } {
    if (pos >= text.length) {
        throw new Error("Unexpected end of escape sequence");
    }

    let newPos = pos + 1;
    let result: string;

    switch (text[pos]) {
        case 'b': result = '\b'; break;
        case 'f': result = '\f'; break;
        case 'n': result = '\n'; break;
        case 'r': result = '\r'; break;
        case 't': result = '\t'; break;
        case '0': result = '\0'; break;
        case '\'': result = '\''; break;
        case '\"': result = '\"'; break;
        case '\\': result = '\\'; break;
        default: 
            // Just return the character as-is if not a recognized escape sequence
            result = text[pos];
    }

    return { char: result, newPos };
}

/**
 * Reads a double-quoted string with SQL style quoting
 */
export function readDoubleQuotedStringWithSQLStyle(text: string): string {
    return readAnyQuotedString(text, '"', true);
}

/**
 * Reads a back-quoted string with SQL style quoting
 */
export function readBackQuotedStringWithSQLStyle(text: string): string {
    return readAnyQuotedString(text, '`', true);
}