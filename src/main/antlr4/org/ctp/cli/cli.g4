grammar cli;

commands : command+;

command :
    put_command
    | get_command
    | delete_command
    | cas_command
    | flush_command
    | engine_command
    | exit_command
    ;

put_command :
    'put' KEYSTRING  ',' VALUESTRING LINEEND
    ;

get_command :
    'get' KEYSTRING LINEEND
    ;

delete_command :
    'delete' KEYSTRING LINEEND
    ;

cas_command :
    'cas' KEYSTRING  ',' VALUESTRING  ',' VALUESTRING LINEEND
    ;

flush_command :
    'flush' LINEEND
    ;

engine_command :
    'engine' LINEEND
    ;

exit_command :
    'exit' LINEEND
    ;

WS : [ \t] -> skip;
LINEEND : [\r\n]+ ;

VALUESTRING : '"' SCharSequence? '"' ;

KEYSTRING : [A-Za-z0-9_]+ ;

fragment
SCharSequence
    :   SChar+
    ;

fragment
SChar
    :   ~["\\\r\n]
    |   EscapeSequence
;

fragment
EscapeSequence
    :   SimpleEscapeSequence

    ;
fragment
SimpleEscapeSequence
    :   '\\' ['"?abfnrtv\\]
    ;
