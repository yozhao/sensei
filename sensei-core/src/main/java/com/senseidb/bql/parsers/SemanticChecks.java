package com.senseidb.bql.parsers;

import com.senseidb.bql.parsers.BQLv4Parser.StatementContext;

/**
 *
 * @author Sam Harwell
 */
public class SemanticChecks {
    private static void runSemanticChecks(StatementContext context) {
        disallowMultipleClauses(context);
    }

    private static void disallowMultipleClauses(StatementContext context) {
        MultipleClauseCheck.INSTANCE.visit(context);
    }

    private static class MultipleClauseCheck extends BQLv4BaseVisitor<Void> {
        public static final MultipleClauseCheck INSTANCE = new MultipleClauseCheck();

        @Override
        public Void visitSelect_stmt(BQLv4Parser.Select_stmtContext ctx) {
            if (ctx.order_by_clause().size() > 1) {
                throw new IllegalStateException("ORDER BY clause can only appear once.");
            }

            if (ctx.limit_clause().size() > 1) {
                throw new IllegalStateException("LIMIT clause can only appear once.");
            }

            if (ctx.group_by_clause().size() > 1) {
                throw new IllegalStateException("GROUP BY clause can only appear once.");
            }

            if (ctx.distinct_clause().size() > 1) {
                throw new IllegalStateException("DISTINCT clause can only appear once.");
            }

            if (ctx.execute_clause().size() > 1) {
                throw new IllegalStateException("EXECUTE clause can only appear once.");
            }

            if (ctx.browse_by_clause().size() > 1) {
                throw new IllegalStateException("BROWSE BY clause can only appear once.");
            }

            if (ctx.fetching_stored_clause().size() > 1) {
                throw new IllegalStateException("FETCHING STORED clause can only appear once.");
            }

            if (ctx.route_by_clause().size() > 1) {
                throw new IllegalStateException("ROUTE BY clause can only appear once.");
            }

            if (ctx.relevance_model_clause().size() > 1) {
                throw new IllegalStateException("USING RELEVANCE MODEL clause can only appear once.");
            }

            return null;
        }

    }
}
