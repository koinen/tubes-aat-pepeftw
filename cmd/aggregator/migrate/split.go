package migrate

import "strings"

func splitStatements(script string) []string {
	var (
		statements []string
		current    strings.Builder
		inString   bool
		inComment  bool
	)

	for i := 0; i < len(script); i++ {
		ch := script[i]

		switch {
		case inComment:
			if ch == '\n' {
				inComment = false
				current.WriteByte(ch)
			}
			continue
		case inString:
			current.WriteByte(ch)
			if ch == '\'' {
				inString = false
			}
			continue
		case ch == '-' && i+1 < len(script) && script[i+1] == '-':
			inComment = true
			i++
			continue
		case ch == '\'':
			inString = true
		case ch == ';':
			statements = appendStatement(statements, current.String())
			current.Reset()
			continue
		}
		current.WriteByte(ch)
	}

	return appendStatement(statements, current.String())
}

func appendStatement(statements []string, raw string) []string {
	if s := strings.TrimSpace(raw); s != "" {
		return append(statements, s)
	}
	return statements
}
