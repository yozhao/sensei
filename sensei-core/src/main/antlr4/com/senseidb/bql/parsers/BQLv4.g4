/*******************************************************************************************

BNF Grammar for BQL
===================

<statement> ::= ( <select_stmt> | <describe_stmt> ) [';']

<select_stmt> ::= SELECT <select_list> [<from_clause>] [<where_clause>] [<given_clause>]
                  [<additional_clauses>]

<describe_stmt> ::= ( DESC | DESCRIBE ) [<index_name>]

<select_list> ::= '*' | (<column_name>|<aggregation_function>)( ',' <column_name>|<aggregation_function> )*

<column_name_list> ::= <column_name> ( ',' <column_name> )*

<aggregation_function> ::= <function_name> '(' <column_name> ')'

<function_name> ::= <column_name>

<from_clause> ::= FROM <index_name>

<index_name> ::= <identifier> | <quoted_string>

<where_clause> ::= WHERE <search_expr>

<search_expr> ::= <term_expr> ( OR <term_expr> )*

<term_expr> ::= <facet_expr> ( AND <facet_expr> )*

<facet_expr> ::= <predicate> 
               | '(' <search_expr> ')'

<predicates> ::= <predicate> ( AND <predicate> )*

<predicate> ::= <in_predicate>
              | <contains_all_predicate>
              | <equal_predicate>
              | <not_equal_predicate>
              | <query_predicate>
              | <between_predicate>
              | <range_predicate>
              | <time_predicate>
              | <match_predicate>
              | <like_predicate>
              | <null_predicate>

<in_predicate> ::= <column_name> [NOT] IN <value_list> [<except_clause>] [<predicate_props>]

<contains_all_predicate> ::= <column_name> CONTAINS ALL <value_list> [<except_clause>]
                             [<predicate_props>]

<equal_predicate> ::= <column_name> '=' <value> [<predicate_props>]

<not_equal_predicate> ::= <column_name> '<>' <value> [<predicate_props>]

<query_predicate> ::= QUERY IS <quoted_string>

<between_predicate> ::= <column_name> [NOT] BETWEEN <value> AND <value>

<range_predicate> ::= <column_name> <range_op> <numeric>

<time_predicate> ::= <column_name> IN LAST <time_span>
                   | <column_name> ( SINCE | AFTER | BEFORE ) <time_expr>

<match_predicate> ::= [NOT] MATCH '(' <column_name_list> ')' AGAINST '(' <quoted_string> ')'

<like_predicate> ::= <column_name> [NOT] LIKE <quoted_string>

<null_predicate> ::= <column_name> IS [NOT] NULL

<value_list> ::= <non_variable_value_list> | <variable>

<non_variable_value_list> ::= '(' <value> ( ',' <value> )* ')'

<python_style_list> ::= '[' <python_style_value>? ( ',' <python_style_value> )* ']'

<python_style_dict> ::= '{''}' 
                       | '{' <key_value_pair> ( ',' <key_value_pair> )* '}'

<python_style_value> ::= <value>
                       | <python_style_list>
                       | <python_style_dict>

<value> ::= <numeric>
          | <quoted_string>
          | TRUE
          | FALSE
          | <variable>

<range_op> ::= '<' | '<=' | '>=' | '>'

<except_clause> ::= EXCEPT <value_list>

<predicate_props> ::= WITH <prop_list>

<prop_list> ::= '(' <key_value_pair> ( ',' <key_value_pair> )* ')'

<key_value_pair> ::= <quoted_string> ':' 
                     ( <value> | <python_style_list> | <python_style_dict> )

<given_clause> ::= GIVEN FACET PARAM <facet_param_list>

<facet_param_list> ::= <facet_param> ( ',' <facet_param> )*

<facet_param> ::= '(' <facet_name> <facet_param_name> <facet_param_type> <facet_param_value> ')'

<facet_param_name> ::= <quoted_string>

<facet_param_type> ::= BOOLEAN | INT | LONG | STRING | BYTEARRAY | DOUBLE

<facet_param_value> ::= <quoted_string>

<additional_clauses> ::= ( <additional_clause> )+

<additional_clause> ::= <order_by_clause>
                      | <limit_clause>
                      | <group_by_clause>
                      | <distinct_clause>
                      | <execute_clause>
                      | <browse_by_clause>
                      | <fetching_stored_clause>
                      | <route_by_clause>
                      | <relevance_model_clause>

<order_by_clause> ::= ORDER BY <sort_specs>

<sort_specs> ::= <sort_spec> ( ',' <sort_spec> )*

<sort_spec> ::= <column_name> [<ordering_spec>]

<ordering_spec> ::= ASC | DESC

<group_by_clause> ::= GROUP BY <group_spec>

<distinct_clause> ::= DISTINCT <distinct_spec>

<execute_clause> ::= EXECUTE '(' function_name ((',' python_style_dict) | (',' key_value_pair)*) ')'

<group_spec> ::= <comma_column_name_list> [TOP <max_per_group>]

<distinct_spec> ::= <or_column_name_list>

<or_column_name_list> ::= <column_name> ( OR <column_name> )*

<comma_column_name_list> ::= <column_name> ( (OR | ',') <column_name> )*

<limit_clause> ::= LIMIT [<offset> ','] <count>

<offset> ::= ( <digit> )+

<count> ::= ( <digit> )+

<browse_by_clause> ::= BROWSE BY <facet_specs>

<facet_specs> ::= <facet_spec> ( ',' <facet_spec> )*

<facet_spec> ::= <facet_name> [<facet_expression>]

<facet_expression> ::= '(' <expand_flag> <count> <count> <facet_ordering> ')'

<expand_flag> ::= TRUE | FALSE

<facet_ordering> ::= HITS | VALUE

<fetching_stored_clause> ::= FETCHING STORED [<fetching_flag>]

<fetching_flag> ::= TRUE | FALSE

<route_by_clause> ::= ROUTE BY <quoted_string>

<relevance_model_clause> ::= USING RELEVANCE MODEL <identifier> <prop_list>
                             [<relevance_model>]

<relevance_model> ::= DEFINED AS <formal_parameters> BEGIN <model_block> END

<formal_parameters> ::= '(' <formal_parameter_decls> ')'

<formal_parameter_decls> ::= <formal_parameter_decl> ( ',' <formal_parameter_decl> )*

<formal_parameter_decl> ::= <variable_modifiers> <type> <variable_declarator_id>

<variable_modifiers> ::= ( <variable_modifier> )*

<variable_modifier> ::= 'final'

<type> ::= <class_or_interface_type> ('[' ']')*
         | <primitive_type> ('[' ']')*
         | <boxed_type> ('[' ']')*
         | <limited_type> ('[' ']')*

<class_or_interface_type> ::= <fast_util_data_type>

<fast_util_data_type> ::= 'IntOpenHashSet'
                        | 'FloatOpenHashSet'
                        | 'DoubleOpenHashSet'
                        | 'LongOpenHashSet'
                        | 'ObjectOpenHashSet'
                        | 'Int2IntOpenHashMap'
                        | 'Int2FloatOpenHashMap'
                        | 'Int2DoubleOpenHashMap'
                        | 'Int2LongOpenHashMap'
                        | 'Int2ObjectOpenHashMap'
                        | 'Object2IntOpenHashMap'
                        | 'Object2FloatOpenHashMap'
                        | 'Object2DoubleOpenHashMap'
                        | 'Object2LongOpenHashMap'
                        | 'Object2ObjectOpenHashMap'

<primitive_type> ::= 'boolean' | 'char' | 'byte' | 'short' 
                   | 'int' | 'long' | 'float' | 'double'

<boxed_type> ::= 'Boolean' | 'Character' | 'Byte' | 'Short' 
               | 'Integer' | 'Long' | 'Float' | 'Double'

<limited_type> ::= 'String' | 'System' | 'Math'

<model_block> ::= ( <block_statement> )+

<block_statement> ::= <local_variable_declaration_stmt>
                    | <java_statement>

<local_variable_declaration_stmt> ::= <local_variable_declaration> ';'

<local_variable_declaration> ::= <variable_modifiers> <type> <variable_declarators>

<java_statement> ::= <block>
                   | 'if' <par_expression> <java_statement> [ <else_statement> ]
                   | 'for' '(' <for_control> ')' <java_statement>
                   | 'while' <par_expression> <java_statement>
                   | 'do' <java_statement> 'while> <par_expression> ';'
                   | 'switch' <par_expression> '{' <switch_block_statement_groups> '}'
                   | 'return' <expression> ';'
                   | 'break' [<identifier>] ';'
                   | 'continue' [<identifier>] ';'
                   | ';'
                   | <statement_expression> ';'

<block> ::= '{' ( <block_statement> )* '}'

<else_statement> ::= 'else' <java_statement>

<switch_block_statement_groups> ::= ( <switch_block_statement_group> )*

<switch_block_statement_group> ::= ( <switch_label> )+ ( <block_statement> )*

<switch_label> ::= 'case' <constant_expression> ':'
                 | 'case' <enum_constant_name> ':'
                 | 'default' ':'

<for_control> ::= <enhanced_for_control>
                | [<for_init>] ';' [<expression>] ';' [<for_update>]

<for_init> ::= <local_variable_declaration>
             | <expression_list>

<enhanced_for_control> ::= <variable_modifiers> <type> <identifier> ':' <expression>

<for_update> ::= <expression_list>

<par_expression> ::= '(' <expression> ')'

<expression_list> ::= <expression> ( ',' <expression> )*

<statement_expression> ::= <expression>

<constant_expression> ::= <expression>

<enum_constant_name> ::= <identifier>

<variable_declarators> ::= <variable_declarator> ( ',' <variable_declarator> )*

<variable_declarator> ::= <variable_declarator_id> '=' <variable_initializer>

<variable_declarator_id> ::= <identifier> ('[' ']')*

<variable_initializer> ::= <array_initializer>
                         | <expression>

<array_initializer> ::= '{' [ <variable_initializer> ( ',' <variable_initializer> )* [','] ] '}'

<expression> ::= <conditional_expression> [ <assignment_operator> <expression> ]

<assignment_operator> ::= '=' | '+=' | '-=' | '*=' | '/=' | '&=' | '|=' | '^=' |
                        | '%=' | '<<=' | '>>>=' | '>>='

<conditional_expression> ::= <conditional_or_expression> [ '?' <expression> ':' <expression> ]

<conditional_or_expression> ::= <conditional_and_expression> ( '||' <conditional_and_expression> )*

<conditional_and_expression> ::= <inclusive_or_expression> ('&&' <inclusive_or_expression> )*

<inclusive_or_expression> ::= <exclusive_or_expression> ('|' <exclusive_or_expression> )*

<exclusive_or_expression> ::= <and_expression> ('^' <and_expression> )*

<and_expression> ::= <equality_expression> ( '&' <equality_expression> )*

<equality_expression> ::= <instanceof_expression> ( ('==' | '!=') <instanceof_expression> )*

<instanceof_expression> ::= <relational_expression> [ 'instanceof' <type> ]

<relational_expression> ::= <shift_expression> ( <relational_op> <shift_expression> )*

<shift_expression> ::= <additive_expression> ( <shift_op> <additive_expression> )*

<relational_op> ::= '<=' | '>=' | '<' | '>'

<shift_op> ::= '<<' | '>>>' | '>>'

<additive_expression> ::= <multiplicative_expression> ( ('+' | '-') <multiplicative_expression> )*

<multiplicative_expression> ::= <unary_expression> ( ( '*' | '/' | '%' ) <unary_expression> )*

<unary_expression> ::= '+' <unary_expression>
                     | '-' <unary_expression>
                     | '++' <unary_expression>
                     | '--' <unary_expression>
                     | <unary_expression_not_plus_minus>

<unary_expression_not_plus_minus> ::= '~' <unary_expression>
                                    | '!' <unary_expression>
                                    | <cast_expression>
                                    | <primary> <selector>* [ ('++'|'--') ]

<cast_expression> ::= '(' <primitive_type> ')' <unary_expression>
                    | '(' (<type> | <expression>) ')' <unary_expression_not_plus_minus>

<primary> ::= <par_expression>
            | <literal>
            | java_method identifier_suffix
            | <java_ident> ('.' <java_method>)* [<identifier_suffix>]

<java_ident> ::= <boxed_type>
               | <limited_type>
               | <identifier>

<java_method> ::= <identifier>

<identifier_suffix> ::= ('[' ']')+ '.' 'class'
                      | <arguments>
                      | '.' 'class'
                      | '.' 'this'
                      | '.' 'super' <arguments>

<literal> ::= <integer>
            | <real>
            | <floating_point_literal>
            | <character_literal>
            | <quoted_string>
            | <boolean_literal>
            | 'null'

<boolean_literal> ::= 'true' | 'false'

<selector> ::= '.' <identifier> <arguments>
             | '.' 'this'
             | '[' <expression> ']'

<arguments> ::= '(' [<expression_list>] ')'

<quoted_string> ::= '"' ( <char> )* '"'
                  | "'" ( <char> )* "'"

<identifier> ::= <identifier_start> ( <identifier_part> )*

<identifier_start> ::= <alpha> | '-' | '_'

<identifier_part> ::= <identifier_start> | <digit>

<variable> ::= '$' ( <alpha> | <digit> | '_' )+

<column_name> ::= <identifier> | <quoted_string>

<facet_name> ::= <identifier>

<alpha> ::= <alpha_lower_case> | <alpha_upper_case>

<alpha_upper_case> ::= A | B | C | D | E | F | G | H | I | J | K | L | M | N | O
                     | P | Q | R | S | T | U | V | W | X | Y | Z

<alpha_lower_case> ::= a | b | c | d | e | f | g | h | i | j | k | l | m | n | o
                     | p | q | r | s | t | u | v | w | x | y | z

<digit> ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9

<numeric> ::= <time_expr> 
            | <integer>
            | <real>

<integer> ::= ( <digit> )+

<real> ::= ( <digit> )+ '.' ( <digit> )+

<time_expr> ::= <time_span> AGO
              | <date_time_string>
              | NOW

<time_span> ::= [<time_week_part>] [<time_day_part>] [<time_hour_part>]
                [<time_minute_part>] [<time_second_part>] [<time_millisecond_part>]

<time_week_part> ::= <integer> ( 'week' | 'weeks' )

<time_day_part>  ::= <integer> ( 'day'  | 'days' )

<time_hour_part> ::= <integer> ( 'hour' | 'hours' )

<time_minute_part> ::= <integer> ( 'minute' | 'minutes' | 'min' | 'mins')

<time_second_part> ::= <integer> ( 'second' | 'seconds' | 'sec' | 'secs')

<time_millisecond_part> ::= <integer> ( 'millisecond' | 'milliseconds' | 'msec' | 'msecs')

<date_time_string> ::= <date> [<time>]

<date> ::= <digit><digit><digit><digit> ('-' | '/' | '.') <digit><digit>
           ('-' | '/' | '.') <digit><digit>

<time> ::= DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT

*******************************************************************************************/

grammar BQLv4;

// Imaginary tokens
tokens 
{
    COLUMN_LIST,
    OR_PRED,
    AND_PRED,
    EQUAL_PRED,
    RANGE_PRED
}

// As the generated lexer will reside in com.senseidb.bql.parsers package,
// we have to add package declaration on top of it
@lexer::header {
}

@lexer::members {

  // @Override
  // public void reportError(RecognitionException e) {
  //   throw new IllegalArgumentException(e);
  // }

}

// As the generated parser will reside in com.senseidb.bql.parsers
// package, we have to add package declaration on top of it
@parser::header {
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;
import com.senseidb.util.Pair;
import com.senseidb.search.req.BQLParserUtils;
}

@parser::members {

    private static enum KeyType {
      STRING_LITERAL,
      IDENT,
      STRING_LITERAL_AND_IDENT
    }

    // The following two overridden methods are used to force ANTLR to
    // stop parsing upon the very first error.

    // @Override
    // protected void mismatch(IntStream input, int ttype, BitSet follow)
    //     throws RecognitionException
    // {
    //     throw new MismatchedTokenException(ttype, input);
    // }

    @Override
    protected Object recoverFromMismatchedToken(IntStream input, int ttype, BitSet follow)
        throws RecognitionException
    {
        throw new MismatchedTokenException(ttype, input);
    }

    @Override
    public Object recoverFromMismatchedSet(IntStream input,
                                           RecognitionException e,
                                           BitSet follow)
        throws RecognitionException
    {
        throw e;
    }

    @Override
    public String getErrorMessage(RecognitionException err, String[] tokenNames) 
    {
        List stack = getRuleInvocationStack(err, this.getClass().getName());
        String msg = null; 
        if (err instanceof NoViableAltException) {
            NoViableAltException nvae = (NoViableAltException) err;
            // msg = "No viable alt; token=" + err.token.getText() +
            //     " (decision=" + nvae.decisionNumber +
            //     " state "+nvae.stateNumber+")" +
            //     " decision=<<" + nvae.grammarDecisionDescription + ">>";
            msg = "[line:" + err.line + ", col:" + err.charPositionInLine + "] " +
                "No viable alternative (token=" + err.token.getText() + ")" + " (stack=" + stack + ")";
        }
        else if (err instanceof MismatchedTokenException) {
            MismatchedTokenException mte = (MismatchedTokenException) err;
            String tokenName = (mte.expecting == Token.EOF) ? "EOF" : tokenNames[mte.expecting];
            msg = "[line:" + mte.line + ", col:" + mte.charPositionInLine + "] " +
                "Expecting " + tokenName +
                " (token=" + err.token.getText() + ")";
        }
        else if (err instanceof FailedPredicateException) {
            FailedPredicateException fpe = (FailedPredicateException) err;
            msg = "[line:" + fpe.line + ", col:" + fpe.charPositionInLine + "] " +
                fpe.predicateText +
                " (token=" + fpe.token.getText() + ")";
        }
        else if (err instanceof MismatchedSetException) {
            MismatchedSetException mse = (MismatchedSetException) err;
            msg = "[line:" + mse.line + ", col:" + mse.charPositionInLine + "] " +
                "Mismatched input (token=" + mse.token.getText() + ")";
        }
        else {
            msg = super.getErrorMessage(err, tokenNames); 
        }
        return msg;
    } 

    @Override
    public String getTokenErrorDisplay(Token t)
    {
        return t.toString();
    }

}

// ***************** parser rules:

statement
    :   (   select_stmt
        |   describe_stmt
        )   SEMI? EOF
    ;

select_stmt
    :   SELECT ('*' | cols=selection_list)
        (FROM (IDENT | STRING_LITERAL))?
        w=where?
        given=given_clause?
        (   order_by = order_by_clause 
        |   limit = limit_clause
        |   group_by = group_by_clause
        |   distinct = distinct_clause
        |   executeMapReduce = execute_clause
        |   browse_by = browse_by_clause
        |   fetch_stored = fetching_stored_clause
        |   route_param = route_by_clause
        |   rel_model = relevance_model_clause
        )*
    ;

describe_stmt
    :   DESCRIBE (IDENT | STRING_LITERAL)
    ;

selection_list
    :   (   col=column_name
        |   agrFunction=aggregation_function 
        )
        (   COMMA
            (   col=column_name
            |   agrFunction=aggregation_function
            )
        )*
    ;

aggregation_function
 :   (id=function_name LPAR (columnVar=column_name | '*') RPAR)
 ;

column_name
    :   (id=IDENT | str=STRING_LITERAL)
        ('.' (id2=IDENT | str2=STRING_LITERAL)
        )*
    ;

function_name
    :   (min= 'min'|colName=column_name)
    ;

where
    :   WHERE search_expr
    ;

order_by_clause
    :   ORDER BY (RELEVANCE | sort_specs)
    ;

sort_specs
    :   sort=sort_spec
        (COMMA sort=sort_spec   // It's OK to use variable sort again here
        )*
    ;

sort_spec
    :   column_name ordering=(ASC | DESC)?
    ;

limit_clause
    :   LIMIT (n1=INTEGER COMMA)? n2=INTEGER
    ;

comma_column_name_list
    :   col=column_name
        ((OR | COMMA) col=column_name
        )*
    ;

or_column_name_list
    :   col=column_name
        (OR col=column_name
        )*
    ;

group_by_clause
    :   GROUP BY comma_column_name_list (TOP top=INTEGER)?
    ;

distinct_clause
    :   DISTINCT or_column_name_list
    ;

browse_by_clause
    :   BROWSE BY f=facet_spec
        (COMMA f=facet_spec
        )*
    ;

execute_clause
    :   EXECUTE LPAR funName=function_name
        (   COMMA map=python_style_dict
        |   (   COMMA p=key_value_pair[KeyType.STRING_LITERAL]
            )*
        )
        RPAR 
    ;

facet_spec
    :   column_name
        (
            LPAR 
            (TRUE | FALSE) COMMA
            n1=INTEGER COMMA
            n2=INTEGER COMMA
            (HITS | VALUE)
            RPAR
        )*
    ;

fetching_stored_clause
    :   FETCHING STORED
        (   TRUE
        |   FALSE
        )*
    ;

route_by_clause
    :   ROUTE BY STRING_LITERAL 
    ;

search_expr
    :   t=term_expr
        (OR t=term_expr)*
    ;

term_expr
    :   f=factor_expr
        (AND f=factor_expr)*
    ;

factor_expr
    :   predicate
    |   LPAR search_expr RPAR
    ;

predicate
    :   in_predicate
    |   contains_all_predicate
    |   equal_predicate
    |   not_equal_predicate
    |   query_predicate
    |   between_predicate
    |   range_predicate
    |   time_predicate
    |   match_predicate
    |   like_predicate
    |   null_predicate
    |   empty_predicate
    ;

in_predicate
    :   column_name not=NOT? IN value_list except=except_clause? predicate_props?
    ;

empty_predicate
    :   value_list IS (NOT)? EMPTY
    ;
    
contains_all_predicate
    :   column_name CONTAINS ALL value_list except=except_clause? predicate_props? 
    ;

equal_predicate
    :   column_name EQUAL value props=predicate_props?
    ;

not_equal_predicate
    :   column_name NOT_EQUAL value predicate_props?
    ;

query_predicate
    :   QUERY IS STRING_LITERAL
    ;

between_predicate
    :   column_name not=NOT? BETWEEN val1=value AND val2=value
    ;

range_predicate
    :   column_name op=(GT | GTE | LT | LTE) val=value
    ;

time_predicate
    :   column_name (NOT)? IN LAST time_span
    |   column_name (NOT)? (since=SINCE | since=AFTER | before=BEFORE) time_expr
    ;

time_span
    :   week=time_week_part? day=time_day_part? hour=time_hour_part? 
        minute=time_minute_part? second=time_second_part? msec=time_millisecond_part?
    ;

time_week_part
    :   INTEGER WEEKS
    ;

time_day_part
    :   INTEGER DAYS
    ;

time_hour_part
    :   INTEGER HOURS
    ;

time_minute_part
    :   INTEGER (MINUTES | MINS)
    ;

time_second_part
    :   INTEGER (SECONDS | SECS)
    ;

time_millisecond_part
    :   INTEGER (MILLISECONDS | MSECS)
    ;

time_expr
    :   time_span AGO
    |   date_time_string
    |   NOW
    ;

date_time_string
    :   DATE TIME?
    ;

match_predicate
    :   (NOT)? MATCH LPAR selection_list RPAR AGAINST LPAR STRING_LITERAL RPAR
    ;

like_predicate
    :   column_name (NOT)? LIKE STRING_LITERAL
    ;

null_predicate
    :   column_name IS (NOT)? NULL
    ;

non_variable_value_list
    :   LPAR v=value
        (   COMMA v=value
        )*
        RPAR
    |   LPAR RPAR
    ;

python_style_list
    :   '[' v=python_style_value?
        (   COMMA v=python_style_value
        )*
        ']'
    ;

python_style_dict
    :   '{' '}'
    |   '{' p=key_value_pair[KeyType.STRING_LITERAL]
        (   COMMA p=key_value_pair[KeyType.STRING_LITERAL]
        )*
        '}'
    ;

python_style_value
    :   value
    |   python_style_list
    |   python_style_dict
    ;

value_list
    :   non_variable_value_list
    |   VARIABLE
    ;

value
    :   numeric
    |   STRING_LITERAL
    |   TRUE
    |   FALSE
    |   VARIABLE
    ;

numeric
    :   time_expr
    |   INTEGER
    |   REAL
    ;

except_clause
    :   EXCEPT value_list
    ;
  
predicate_props
    :   WITH prop_list[KeyType.STRING_LITERAL]
    ;

prop_list[KeyType keyType]
    :   LPAR p=key_value_pair[keyType]
        (   COMMA p=key_value_pair[keyType]
        )*
        RPAR
    ;

key_value_pair[KeyType keyType]
    :   ( { $keyType == KeyType.STRING_LITERAL ||
            $keyType == KeyType.STRING_LITERAL_AND_IDENT}? STRING_LITERAL
        | { $keyType == KeyType.IDENT ||
            $keyType == KeyType.STRING_LITERAL_AND_IDENT}? IDENT
        )
        COLON (v=value | vs=python_style_list | vd=python_style_dict)
    ;

given_clause
    :   GIVEN FACET PARAM facet_param_list
    ;


// =====================================================================
// Relevance model related
// =====================================================================

variable_declarators
    :   var1=variable_declarator
        (COMMA var2=variable_declarator
        )*
    ;

variable_declarator
    :   variable_declarator_id ('=' variable_initializer)?
    ;

variable_declarator_id
    :   IDENT ('[' ']')*
    ;

variable_initializer
    :   array_initializer
    |   expression
    ;

array_initializer
    :   '{' (variable_initializer (',' variable_initializer)* (',')?)? '}'
    ;

type
    :   class_or_interface_type ('[' ']')*
    |   primitive_type ('[' ']')*
    |   boxed_type ('[' ']')*
    |   limited_type ('[' ']')*
    ;

class_or_interface_type
    :   FAST_UTIL_DATA_TYPE
    ;

type_arguments
    :   '<'
        ta1=type_argument
        (COMMA ta2=type_argument
        )*
        '>'
    ;

type_argument
    :   type
    |   '?' (('extends' | 'super') type)?
    ;

formal_parameters
    :   LPAR formal_parameter_decls RPAR
    ;

formal_parameter_decls
    :   decl=formal_parameter_decl
        (COMMA decl=formal_parameter_decl
        )*
    ;
    
formal_parameter_decl
    :   variable_modifiers type variable_declarator_id
    ;

primitive_type
    :   { "boolean".equals(input.LT(1).getText()) }? BOOLEAN
    |   'char'
    |   { "byte".equals(input.LT(1).getText()) }? BYTE
    |   'short'
    |   { "int".equals(input.LT(1).getText()) }? INT
    |   { "long".equals(input.LT(1).getText()) }? LONG
    |   'float'
    |   { "double".equals(input.LT(1).getText()) }? DOUBLE
    ;

boxed_type
    :   { "Boolean".equals(input.LT(1).getText()) }? BOOLEAN
    |   'Character'
    |   { "Byte".equals(input.LT(1).getText()) }? BYTE
    |   'Short'
    |   'Integer'
    |   { "Long".equals(input.LT(1).getText()) }? LONG
    |   'Float'
    |   { "Double".equals(input.LT(1).getText()) }? DOUBLE
    ;

limited_type
    :   'String'
    |   'System'
    |   'Math'     
    ;

variable_modifier
    :   'final'
    ;

relevance_model
    :   DEFINED AS params=formal_parameters
        BEGIN model_block END
    ;

model_block
    :   block_statement+
    ;

block
    :   '{' 
        block_statement* 
        '}'
    ;

block_statement
    :   local_variable_declaration_stmt
    |   java_statement
    ;

local_variable_declaration_stmt
    :   local_variable_declaration SEMI
    ;

local_variable_declaration
    :   variable_modifiers type variable_declarators
    ;

variable_modifiers
    :   variable_modifier*
    ;

java_statement
    :   block
    |   'if' par_expression java_statement (else_statement)?
    |   FOR LPAR
        for_control RPAR java_statement
    |   'while' par_expression java_statement
    |   'do' java_statement 'while' par_expression SEMI
    |   'switch' par_expression '{' switch_block_statement_groups '}'
    |   'return' expression SEMI
    |   'break' IDENT? SEMI
    |   'continue' IDENT? SEMI
    |   SEMI
    |   statement_expression SEMI
    ;

else_statement
    :   { "else".equals(input.LT(1).getText()) }? ELSE java_statement
    ;

switch_block_statement_groups
    :   (switch_block_statement_group)*
    ;

switch_block_statement_group
    :   switch_label+ block_statement*
    ;

switch_label
    :   'case' constant_expression COLON
    |   'case' enum_constant_name COLON
    |   'default' COLON
    ;

for_control
    :   enhanced_for_control
    |   for_init? SEMI expression? SEMI for_update?
    ;

for_init
    :   local_variable_declaration
    |   expression_list
    ;

enhanced_for_control
    :   variable_modifiers type IDENT COLON expression
    ;

for_update
    :   expression_list
    ;

par_expression
    :   LPAR expression RPAR
    ;

expression_list
    :   expression (',' expression)*
    ;

statement_expression
    :   expression
    ;

constant_expression
    :   expression
    ;

enum_constant_name
    :   IDENT
    ;

expression
    :   conditional_expression (assignment_operator expression)?
    ;

assignment_operator
    :   '='
    |   '+='
    |   '-='
    |   '*='
    |   '/='
    |   '&='
    |   '|='
    |   '^='
    |   '%='
    |   '<' '<' '=' 
    |   '>' '>' '>' '='
    |   '>' '>' '='
    ;

conditional_expression
    :   conditional_or_expression ( '?' expression ':' expression )?
    ;

conditional_or_expression
    :   conditional_and_expression ( '||' conditional_and_expression )*
    ;

conditional_and_expression
    :   inclusive_or_expression ('&&' inclusive_or_expression )*
    ;

inclusive_or_expression
    :   exclusive_or_expression ('|' exclusive_or_expression )*
    ;

exclusive_or_expression
    :   and_expression ('^' and_expression )*
    ;

and_expression
    :   equality_expression ( '&' equality_expression )*
    ;

equality_expression
    :   instanceof_expression ( ('==' | '!=') instanceof_expression )*
    ;

instanceof_expression
    :   relational_expression ('instanceof' type)?
    ;

relational_expression
    :   shift_expression ( relational_op shift_expression )*
    ;

relational_op
    :   '<' '=' 
    |   '>' '=' 
    |   '<'
    |   '>'
    ;

shift_expression
    :   additive_expression ( shift_op additive_expression )*
    ;

shift_op
    :   '<' '<' 
    |   '>' '>' '>' 
    |   '>' '>'
    ;

additive_expression
    :   multiplicative_expression ( ('+' | '-') multiplicative_expression )*
    ;

multiplicative_expression
    :   unary_expression ( ( '*' | '/' | '%' ) unary_expression )*
    ;
    
unary_expression
    :   '+' unary_expression
    |   '-' unary_expression
    |   '++' unary_expression
    |   '--' unary_expression
    |   unary_expression_not_plus_minus
    ;

unary_expression_not_plus_minus
    :   '~' unary_expression
    |   '!' unary_expression
    |   cast_expression
    |   primary selector* ('++'|'--')?
    ;

cast_expression
    :  '(' primitive_type ')' unary_expression
    |  '(' (type | expression) ')' unary_expression_not_plus_minus
    ;

primary
    :   par_expression
    |   literal   
    |   java_method identifier_suffix
    |   java_ident ('.' java_method)* identifier_suffix?
    ;

java_ident
    :   boxed_type
    |   limited_type
    |   IDENT
    ;

// Need to handle the conflicts of BQL keywords and common Java method
// names supported by BQL.
java_method
    :   { "contains".equals(input.LT(1).getText()) }? CONTAINS
    |   IDENT
    ;

identifier_suffix
    :   ('[' ']')+ '.' 'class'
    |   arguments
    |   '.' 'class'
    |   '.' 'this'
    |   '.' 'super' arguments
    ;

literal 
    :   integer_literal
    |   REAL
    |   FLOATING_POINT_LITERAL
    |   CHARACTER_LITERAL
    |   STRING_LITERAL
    |   boolean_literal
    |   { "null".equals(input.LT(1).getText()) }? NULL
    ;

integer_literal
    :   HEX_LITERAL
    |   OCTAL_LITERAL
    |   INTEGER
    ;

boolean_literal
    :   { "true".equals(input.LT(1).getText()) }? TRUE
    |   { "false".equals(input.LT(1).getText()) }? FALSE
    ;

selector
    :   '.' IDENT arguments?
    |   '.' 'this'
    |   '[' expression ']'
    ;

arguments
    :   '(' expression_list? ')'
    ;
    
relevance_model_clause
    :   USING RELEVANCE MODEL IDENT prop_list[KeyType.STRING_LITERAL_AND_IDENT] model=relevance_model?
    ;

facet_param_list
    :   p=facet_param
        (   COMMA p=facet_param
        )*
    ;

facet_param
    :   LPAR column_name COMMA STRING_LITERAL COMMA facet_param_type COMMA (val=value | valList=non_variable_value_list) RPAR
    ;

facet_param_type
    :   t=(BOOLEAN | INT | LONG | STRING | BYTEARRAY | DOUBLE) 
    ;


fragment DIGIT : '0'..'9' ;
fragment ALPHA : 'a'..'z' | 'A'..'Z' ;

INTEGER : ('0' | '1'..'9' '0'..'9'*) INTEGER_TYPE_SUFFIX? ;
REAL : DIGIT+ '.' DIGIT* ;
LPAR : '(' ;
RPAR : ')' ;
COMMA : ',' ;
COLON : ':' ;
SEMI : ';' ;
EQUAL : '=' ;
GT : '>' ;
GTE : '>=' ;
LT : '<' ;
LTE : '<=';
NOT_EQUAL : '<>' ;
DOT : '.';
LBRACE : '{';
RBRACE : '}';
LBRACK : '[';
RBRACK : ']';
PLUS : '+';
MINUS : '-';
STAR : '*';
DIV : '/';
MOD : '%';
INC : '++';
DEC : '--';
TILDE : '~';
BANG : '!';
CARET : '^';
EQEQ : '==';
NEQ : '!=';
PLUSEQ : '+=';
MINUSEQ : '-=';
STAREQ : '*=';
DIVEQ : '/=';
AMPEQ : '&=';
PIPEEQ : '|=';
CARETEQ : '^=';
MODEQ : '%=';
OROR : '||';
ANDAND : '&&';
PIPE : '|';
AMP : '&';
QUES : '?';

STRING_LITERAL
    :   ('"'
            { StringBuilder builder = new StringBuilder().appendCodePoint('"'); }
            ('"' '"'               { builder.appendCodePoint('"'); }
            | ch=~('"'|'\r'|'\n')  { builder.appendCodePoint(ch); }
            )*
         '"'
            { setText(builder.appendCodePoint('"').toString()); }
        )
    |
        ('\''
            { StringBuilder builder = new StringBuilder().appendCodePoint('\''); }
            ('\'' '\''             { builder.appendCodePoint('\''); }
            | ch=~('\''|'\r'|'\n') { builder.appendCodePoint(ch); }
            )*
         '\''
            { setText(builder.appendCodePoint('\'').toString()); }
        )
    ;

DATE
    :   DIGIT DIGIT DIGIT DIGIT ('-'|'/') DIGIT DIGIT ('-'|'/') DIGIT DIGIT 
    ;

TIME
    :
        DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT
    ;

//
// BQL Relevance model related
//

fragment HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;
fragment INTEGER_TYPE_SUFFIX: ('l' | 'L') ;
fragment EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;
fragment FLOAT_TYPE_SUFFIX : ('f'|'F'|'d'|'D') ;

fragment
ESCAPE_SEQUENCE
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESCAPE
    |   OCTAL_ESCAPE
    ;

fragment
UNICODE_ESCAPE
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

fragment
OCTAL_ESCAPE
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

HEX_LITERAL : '0' ('x'|'X') HEX_DIGIT+ INTEGER_TYPE_SUFFIX? ;
OCTAL_LITERAL : '0' ('0'..'7')+ INTEGER_TYPE_SUFFIX? ;

FLOATING_POINT_LITERAL
    :   REAL EXPONENT? FLOAT_TYPE_SUFFIX?
    |   '.' DIGIT+ EXPONENT? FLOAT_TYPE_SUFFIX?
    |   DIGIT+ EXPONENT FLOAT_TYPE_SUFFIX?
    |   DIGIT+ FLOAT_TYPE_SUFFIX
    ;

CHARACTER_LITERAL
    :   '\'' ( ESCAPE_SEQUENCE | ~('\''|'\\') ) '\''
    ;

BREAK : 'break';
CASE : 'case';
CHAR : 'char';
CHARACTER : 'Character';
CLASS : 'class';
CONTINUE : 'continue';
DEFAULT : 'default';
DO : 'do';
EXTENDS : 'extends';
FINAL : 'final';
FLOAT : 'float';
FLOAT2 : 'Float';
FOR : 'for';
IF : 'if';
INTEGER2 : 'Integer';
INSTANCEOF : 'instanceof';
MATH : 'Math';
MIN : 'min';
RETURN : 'return';
SHORT : 'short';
SHORT2 : 'Short';
STRING2 : 'String';
SUPER : 'super';
SWITCH : 'switch';
SYSTEM : 'System';
THIS : 'this';
WHILE : 'while';

//
// BQL Keywords
//

ALL : ('A'|'a')('L'|'l')('L'|'l') ;
AFTER : ('A'|'a')('F'|'f')('T'|'t')('E'|'e')('R'|'r') ;
AGAINST : ('A'|'a')('G'|'g')('A'|'a')('I'|'i')('N'|'n')('S'|'s')('T'|'t') ;
AGO : ('A'|'a')('G'|'g')('O'|'o') ;
AND : ('A'|'a')('N'|'n')('D'|'d') ;
AS : ('A'|'a')('S'|'s') ;
ASC : ('A'|'a')('S'|'s')('C'|'c') ;
BEFORE : ('B'|'b')('E'|'e')('F'|'f')('O'|'o')('R'|'r')('E'|'e') ;
BEGIN : ('B'|'b')('E'|'e')('G'|'g')('I'|'i')('N'|'n') ;
BETWEEN : ('B'|'b')('E'|'e')('T'|'t')('W'|'w')('E'|'e')('E'|'e')('N'|'n') ;
BOOLEAN : ('B'|'b')('O'|'o')('O'|'o')('L'|'l')('E'|'e')('A'|'a')('N'|'n') ;
BROWSE : ('B'|'b')('R'|'r')('O'|'o')('W'|'w')('S'|'s')('E'|'e') ;
BY : ('B'|'b')('Y'|'y') ;
BYTE : ('B'|'b')('Y'|'y')('T'|'t')('E'|'e') ;
BYTEARRAY : ('B'|'b')('Y'|'y')('T'|'t')('E'|'e')('A'|'a')('R'|'r')('R'|'r')('A'|'a')('Y'|'y') ;
CONTAINS : ('C'|'c')('O'|'o')('N'|'n')('T'|'t')('A'|'a')('I'|'i')('N'|'n')('S'|'s') ;
DEFINED : ('D'|'d')('E'|'e')('F'|'f')('I'|'i')('N'|'n')('E'|'e')('D'|'d') ;
DESC : ('D'|'d')('E'|'e')('S'|'s')('C'|'c') ;
DESCRIBE : ('D'|'d')('E'|'e')('S'|'s')('C'|'c')('R'|'r')('I'|'i')('B'|'b')('E'|'e') ;
DISTINCT : ('D'|'d')('I'|'i')('S'|'s')('T'|'t')('I'|'i')('N'|'n')('C'|'c')('T'|'t') ;
DOUBLE : ('D'|'d')('O'|'o')('U'|'u')('B'|'b')('L'|'l')('E'|'e') ;
EMPTY : ('E'|'e')('M'|'m')('P'|'p')('T'|'t')('Y'|'y') ;
ELSE : ('E'|'e')('L'|'l')('S'|'s')('E'|'e') ;
END : ('E'|'e')('N'|'n')('D'|'d') ;
EXCEPT : ('E'|'e')('X'|'x')('C'|'c')('E'|'e')('P'|'p')('T'|'t') ;
EXECUTE : ('E'|'e')('X'|'x')('E'|'e')('C'|'c')('U'|'u')('T'|'t')('E'|'e') ;
FACET : ('F'|'f')('A'|'a')('C'|'c')('E'|'e')('T'|'t') ;
FALSE : ('F'|'f')('A'|'a')('L'|'l')('S'|'s')('E'|'e') ;
FETCHING : ('F'|'f')('E'|'e')('T'|'t')('C'|'c')('H'|'h')('I'|'i')('N'|'n')('G'|'g') ;
FROM : ('F'|'f')('R'|'r')('O'|'o')('M'|'m') ;
GROUP : ('G'|'g')('R'|'r')('O'|'o')('U'|'u')('P'|'p') ;
GIVEN : ('G'|'g')('I'|'i')('V'|'v')('E'|'e')('N'|'n') ;
HITS : ('H'|'h')('I'|'i')('T'|'t')('S'|'s') ;
IN : ('I'|'i')('N'|'n') ;
INT : ('I'|'i')('N'|'n')('T'|'t') ;
IS : ('I'|'i')('S'|'s') ;
LAST : ('L'|'l')('A'|'a')('S'|'s')('T'|'t') ;
LIKE : ('L'|'l')('I'|'i')('K'|'k')('E'|'e') ;
LIMIT : ('L'|'l')('I'|'i')('M'|'m')('I'|'i')('T'|'t') ;
LONG : ('L'|'l')('O'|'o')('N'|'n')('G'|'g') ;
MATCH : ('M'|'m')('A'|'a')('T'|'t')('C'|'c')('H'|'h') ;
MODEL : ('M'|'m')('O'|'o')('D'|'d')('E'|'e')('L'|'l') ;
NOT : ('N'|'n')('O'|'o')('T'|'t') ;
NOW : ('N'|'n')('O'|'o')('W'|'w') ;
NULL : ('N'|'n')('U'|'u')('L'|'l')('L'|'l') ;
OR : ('O'|'o')('R'|'r') ;
ORDER : ('O'|'o')('R'|'r')('D'|'d')('E'|'e')('R'|'r') ;
PARAM : ('P'|'p')('A'|'a')('R'|'r')('A'|'a')('M'|'m') ;
QUERY : ('Q'|'q')('U'|'u')('E'|'e')('R'|'r')('Y'|'y') ;
ROUTE : ('R'|'r')('O'|'o')('U'|'u')('T'|'t')('E'|'e') ;
RELEVANCE : ('R'|'r')('E'|'e')('L'|'l')('E'|'e')('V'|'v')('A'|'a')('N'|'n')('C'|'c')('E'|'e') ;
SELECT : ('S'|'s')('E'|'e')('L'|'l')('E'|'e')('C'|'c')('T'|'t') ;
SINCE : ('S'|'s')('I'|'i')('N'|'n')('C'|'c')('E'|'e') ;
STORED : ('S'|'s')('T'|'t')('O'|'o')('R'|'r')('E'|'e')('D'|'d') ;
STRING : ('S'|'s')('T'|'t')('R'|'r')('I'|'i')('N'|'n')('G'|'g') ;
TOP : ('T'|'t')('O'|'o')('P'|'p') ;
TRUE : ('T'|'t')('R'|'r')('U'|'u')('E'|'e') ;
USING : ('U'|'u')('S'|'s')('I'|'i')('N'|'n')('G'|'g') ;
VALUE : ('V'|'v')('A'|'a')('L'|'l')('U'|'u')('E'|'e') ;
WHERE : ('W'|'w')('H'|'h')('E'|'e')('R'|'r')('E'|'e') ;
WITH : ('W'|'w')('I'|'i')('T'|'t')('H'|'h') ;

WEEKS : ('W'|'w')('E'|'e')('E'|'e')('K'|'k')('S'|'s')? ;
DAYS : ('D'|'d')('A'|'a')('Y'|'y')('S'|'s')? ;
HOURS : ('H'|'h')('O'|'o')('U'|'u')('R'|'r')('S'|'s')? ;
MINUTES : ('M'|'m')('I'|'i')('N'|'n')('U'|'u')('T'|'t')('E'|'e')('S'|'s')? ;
MINS : ('M'|'m')('I'|'i')('N'|'n')('S'|'s')? ;
SECONDS : ('S'|'s')('E'|'e')('C'|'c')('O'|'o')('N'|'n')('D'|'d')('S'|'s')? ;
SECS : ('S'|'s')('E'|'e')('C'|'c')('S'|'s')? ;
MILLISECONDS : ('M'|'m')('I'|'i')('L'|'l')('L'|'l')('I'|'i')('S'|'s')('E'|'e')('C'|'c')('O'|'o')('N'|'n')('D'|'d')('S'|'s')? ;
MSECS : ('M'|'m')('S'|'s')('E'|'e')('C'|'c')('S'|'s')? ;

FAST_UTIL_DATA_TYPE
    :   'IntOpenHashSet'
    |   'FloatOpenHashSet'
    |   'DoubleOpenHashSet'
    |   'LongOpenHashSet'
    |   'ObjectOpenHashSet'
    |   'Int2IntOpenHashMap'
    |   'Int2FloatOpenHashMap'
    |   'Int2DoubleOpenHashMap'
    |   'Int2LongOpenHashMap'
    |   'Int2ObjectOpenHashMap'
    |   'Object2IntOpenHashMap'
    |   'Object2FloatOpenHashMap'
    |   'Object2DoubleOpenHashMap'
    |   'Object2LongOpenHashMap'
    |   'Object2ObjectOpenHashMap'
    ;

// Have to define this after the keywords?
IDENT : (ALPHA | '_') (ALPHA | DIGIT | '-' | '_')* ;
VARIABLE : '$' (ALPHA | DIGIT | '_')+ ;

WS : ( ' ' | '\t' | '\r' | '\n' )+ -> channel(HIDDEN);

COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

LINE_COMMENT
    : '--' ~('\n'|'\r')* '\r'? '\n' -> channel(HIDDEN)
    ;
