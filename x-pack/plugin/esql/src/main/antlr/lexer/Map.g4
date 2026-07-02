/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Map;

MAP : 'map' -> pushMode(MAP_MODE);

mode MAP_MODE;
MAP_CROSS     : 'cross';
MAP_ZIP       : 'zip';
// Reuse the shared LP/RP tokens (via type()) instead of defining new '(' / ')' literals.
// Defining a second literal token for '(' / ')' makes ANTLR drop the literal display name
// for LP/RP (error messages would show "RP" instead of "')'"), which regresses unrelated
// parser error-message tests. This mirrors how MMR.g4 and Fork.g4 reuse LP/RP.
MAP_LPAREN    : LP                      -> type(LP);
MAP_RPAREN    : RP                      -> type(RP);
MAP_RETURNING : 'returning';
// [ begins the sub-pipeline: enter DEFAULT_MODE so all commands are parseable.
// The matching ] is CLOSING_BRACKET from Expression.g4, which pops twice and exits MAP_MODE.
MAP_LB        : '['                      -> type(OPENING_BRACKET), pushMode(DEFAULT_MODE);
MAP_PIPE      : PIPE                     -> type(PIPE), popMode;
MAP_OPENING_BRACKET : OPENING_BRACKET   -> type(OPENING_BRACKET);
MAP_CLOSING_BRACKET : CLOSING_BRACKET   -> type(CLOSING_BRACKET);
MAP_DOT       : DOT                      -> type(DOT);
MAP_PARAM     : PARAM                    -> type(PARAM);
MAP_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);
MAP_DOUBLE_PARAMS : DOUBLE_PARAMS -> type(DOUBLE_PARAMS);
MAP_NAMED_OR_POSITIONAL_DOUBLE_PARAMS : NAMED_OR_POSITIONAL_DOUBLE_PARAMS -> type(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);
MAP_QUOTED_IDENTIFIER    : QUOTED_IDENTIFIER    -> type(QUOTED_IDENTIFIER);
MAP_UNQUOTED_IDENTIFIER  : UNQUOTED_IDENTIFIER  -> type(UNQUOTED_IDENTIFIER);
MAP_WS                   : WS                   -> channel(HIDDEN);
MAP_LINE_COMMENT         : LINE_COMMENT         -> channel(HIDDEN);
MAP_MULTILINE_COMMENT    : MULTILINE_COMMENT    -> channel(HIDDEN);
