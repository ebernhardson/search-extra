package org.wikimedia.search.extra.router;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.*;
import org.wikimedia.search.extra.router.AbstractRouterQueryBuilder.Condition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
abstract public class AbstractRouterQueryBuilder<C extends Condition, QB extends AbstractRouterQueryBuilder<C, QB>> extends AbstractQueryBuilder<QB> {
    static final ParseField FALLBACK = new ParseField("fallback");
    static final ParseField CONDITIONS = new ParseField("conditions");
    static final ParseField QUERY = new ParseField("query");

    @Getter(AccessLevel.PRIVATE)
    private List<C> conditions;

    private QueryBuilder fallback;

    AbstractRouterQueryBuilder() {
        this.conditions = new ArrayList<>();
    }

    AbstractRouterQueryBuilder(StreamInput in, Writeable.Reader<C> reader) throws IOException {
        super(in);
        conditions = in.readList(reader);
        fallback = in.readNamedWriteable(QueryBuilder.class);
    }

    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeList(conditions);
        out.writeNamedWriteable(fallback);
    }

    QueryBuilder doRewrite(Predicate<C> condition) throws IOException {
        QueryBuilder qb = conditions.stream()
                .filter(condition)
                .findFirst()
                .map(Condition::query)
                .orElse(fallback);

        if (boost() != DEFAULT_BOOST || queryName() != null) {
            // AbstractQueryBuilder#rewrite will copy non default boost/name
            // to the rewritten query, we pass a fresh BoolQuery so we don't
            // override the one on the rewritten query here
            // Is this really useful?
            return new BoolQueryBuilder().must(qb);
        }
        return qb;

    }

    @Override
    protected boolean doEquals(QB other) {
        AbstractRouterQueryBuilder<C, QB> qb = other;
        return Objects.equals(fallback, qb.fallback) &&
                Objects.equals(conditions, qb.conditions);
    }

    @VisibleForTesting
    Stream<C> conditionStream() {
        return conditions.stream();
    }

    @VisibleForTesting
    protected void condition(C condition) {
        conditions.add(condition);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fallback, conditions);
    }

    @Override
    protected Query doToQuery(QueryShardContext queryShardContext) throws IOException {
        throw new UnsupportedOperationException("This query must be rewritten.");
    }

    protected void addXContent(XContentBuilder builder, Params params) throws IOException {
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getWriteableName());
        if (fallback() != null) {
            builder.field(FALLBACK.getPreferredName(), fallback());
        }
        if (!conditions().isEmpty()) {
            builder.startArray(CONDITIONS.getPreferredName());
            for (C c : conditions()) {
                c.doXContent(builder, params);
            }
            builder.endArray();
        }

        addXContent(builder, params);
        this.printBoostAndQueryName(builder);
        builder.endObject();
    }

    static <C extends Condition, CPS extends ConditionParserState<C>> C parseCondition(
            ObjectParser<CPS, QueryParseContext> condParser, XContentParser parser, QueryParseContext parseContext
    ) throws IOException {
        CPS state = condParser.parse(parser, parseContext);
        String error = state.checkValid();
        if (error != null) {
            throw new ParsingException(parser.getTokenLocation(), error);
        }
        return state.condition();
    }


    @SuppressWarnings("unchecked")
    static <QB extends AbstractRouterQueryBuilder<?, QB>> Optional<QB> fromXContent(
            AbstractObjectParser<QB, QueryParseContext> objectParser, QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        final QB builder;
        try {
            builder = objectParser.parse(parser, parseContext);
        } catch (IllegalArgumentException iae) {
            throw new ParsingException(parser.getTokenLocation(), iae.getMessage());
        }

        final AbstractRouterQueryBuilder<?, QB> qb = builder;
        if (qb.conditions.isEmpty()) {
            throw new ParsingException(parser.getTokenLocation(), "No conditions defined");
        }

        if (qb.fallback == null) {
            throw new ParsingException(parser.getTokenLocation(), "No fallback query defined");
        }

        return Optional.of(builder);
    }

    @Getter
    @Accessors(fluent = true, chain = true)
    @EqualsAndHashCode
    public static class Condition implements Writeable {
        private final ConditionDefinition definition;
        private final int value;
        private final QueryBuilder query;

        Condition(StreamInput in) throws IOException {
            definition = ConditionDefinition.readFrom(in);
            value = in.readVInt();
            query = in.readNamedWriteable(QueryBuilder.class);
        }

        Condition(ConditionDefinition defition, int value, QueryBuilder query) {
            this.definition = Objects.requireNonNull(defition);
            this.value = value;
            this.query = Objects.requireNonNull(query);
        }

        public void writeTo(StreamOutput out) throws IOException {
            definition.writeTo(out);
            out.writeVInt(value);
            out.writeNamedWriteable(query);
        }

        public boolean test(int lhs) {
            return definition.test(lhs, value);
        }

        void addXContent(XContentBuilder builder, Params params) throws IOException {
        }

        void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(definition.parseField.getPreferredName(), value);
            builder.field(QUERY.getPreferredName(), query);
            addXContent(builder, params);
            builder.endObject();
        }
    }

    static <C extends Condition, QB extends AbstractRouterQueryBuilder<C, QB>>
    void declareRouterFields(AbstractObjectParser<QB, QueryParseContext> parser,
                             ContextParser<QueryParseContext, C> objectParser) {
        parser.declareObjectArray(QB::conditions, objectParser, CONDITIONS);
        parser.declareObject(QB::fallback,
                (p, ctx) -> ctx.parseInnerQueryBuilder()
                        .orElseThrow(() -> new ParsingException(p.getTokenLocation(), "No fallback query defined")),
                FALLBACK);
    }

    static <CPS extends ConditionParserState<?>>
    void declareConditionFields(AbstractObjectParser<CPS, QueryParseContext> parser) {
        for (ConditionDefinition def : ConditionDefinition.values()) {
            // gt: int, addPredicate will fail if a predicate has already been set
            parser.declareInt((cps, value) -> cps.addPredicate(def, value), def.parseField);
        }
        // query: { }
        parser.declareObject(CPS::setQuery,
                (p, ctx) -> ctx.parseInnerQueryBuilder()
                        .orElseThrow(() -> new ParsingException(p.getTokenLocation(), "No query defined for condition")),
                QUERY);
    }

    @FunctionalInterface
    interface ConditionProvider<C extends Condition> {
        C create(ConditionDefinition def, int value, QueryBuilder query);
    }

    static class ConditionParserState<C extends Condition> {
        private ConditionProvider<C> provider;
        private ConditionDefinition definition;
        private int value;
        protected QueryBuilder query;

        ConditionParserState(ConditionProvider<C> provider) {
            this.provider = provider;
        }

        void provider(ConditionProvider<C> provider) {
            // Hax because extending classes cant pass a provider that
            // references their own fields before calling the constructor
            this.provider = provider;
        }

        void addPredicate(ConditionDefinition def, int value) {
            if (this.definition != null) {
                throw new IllegalArgumentException("Cannot set extra predicate [" + def.parseField + "] " +
                        "on condition: [" + this.definition.parseField + "] already set");
            }
            this.definition = def;
            this.value = value;
        }

        public void setQuery(QueryBuilder query) {
            this.query = query;
        }

        C condition() {
            Objects.requireNonNull(provider);
            return provider.create(definition, value, query);
        }

        String checkValid() {
            if (query == null) {
                return "Missing field [query] in condition";
            }
            if (definition == null) {
                return "Missing condition predicate in condition";
            }
            return null;
        }
    }

    @FunctionalInterface
    public interface BiIntPredicate {
        boolean test(int a, int b);
    }

    public enum ConditionDefinition implements BiIntPredicate, Writeable {
        eq ((a,b) -> a == b),
        neq ((a,b) -> a != b),
        lte ((a,b) -> a <= b),
        lt ((a,b) -> a < b),
        gte ((a,b) -> a >= b),
        gt ((a,b) -> a > b);

        final ParseField parseField;
        final BiIntPredicate predicate;

        ConditionDefinition(BiIntPredicate predicate) {
            this.predicate = predicate;
            this.parseField = new ParseField(name());
        }

        @Override
        public boolean test(int a, int b) {
            return predicate.test(a, b);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        public static ConditionDefinition readFrom(StreamInput in) throws IOException {
            int ord = in.readVInt();
            if (ord < 0 || ord >= ConditionDefinition.values().length) {
                throw new IOException("Unknown ConditionDefinition ordinal [" + ord + "]");
            }
            return ConditionDefinition.values()[ord];
        }

    }
}
