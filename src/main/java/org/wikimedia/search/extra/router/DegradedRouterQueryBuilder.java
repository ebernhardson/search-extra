package org.wikimedia.search.extra.router;

import com.google.common.annotations.VisibleForTesting;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.monitor.os.OsService;
import org.elasticsearch.monitor.os.OsStats;

import org.wikimedia.search.extra.router.AbstractRouterQueryBuilder.Condition;
import org.wikimedia.search.extra.router.DegradedRouterQueryBuilder.DegradedCondition;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Builds a token_count_router query
 */
@Getter
@Setter
@Accessors(fluent = true, chain = true)
public class DegradedRouterQueryBuilder extends AbstractRouterQueryBuilder<DegradedCondition, DegradedRouterQueryBuilder> {
    public static final ParseField NAME = new ParseField("degraded_router");
    static final ParseField TYPE = new ParseField("type");

    private final static ObjectParser<DegradedRouterQueryBuilder, QueryParseContext> PARSER;
    private final static ObjectParser<DegradedConditionParserState, QueryParseContext> COND_PARSER;

    static {

        COND_PARSER = new ObjectParser<>("condition", DegradedConditionParserState::new);
        COND_PARSER.declareString((cps, value) -> cps.setType(DegradedConditionType.valueOf(value)), TYPE);
        declareConditionFields(COND_PARSER);

        PARSER = new ObjectParser<>(NAME.getPreferredName(), DegradedRouterQueryBuilder::new);
        declareStandardFields(PARSER);
        declareRouterFields(PARSER, (p, pc) -> parseCondition(COND_PARSER, p, pc));
    }

    // This intentional is not considered in doEquals, because it makes
    // testing a pain. Maybe not a great idea though.
    private OsService osService;

    DegradedRouterQueryBuilder() {
        super();
    }

    public DegradedRouterQueryBuilder(StreamInput in, OsService osService) throws IOException {
        super(in, DegradedCondition::new);
        this.osService = osService;
    }

    private static DegradedCondition parseCondition(XContentParser parser, QueryParseContext parseContext) throws IOException {
        return parseCondition(COND_PARSER, parser, parseContext);
    }

    @Override
    public String getWriteableName() {
        return NAME.getPreferredName();
    }

    public static Optional<DegradedRouterQueryBuilder> fromXContent(QueryParseContext parseContext, OsService osService) throws IOException {
        final Optional<DegradedRouterQueryBuilder> builder = AbstractRouterQueryBuilder.fromXContent(PARSER, parseContext);
        builder.ifPresent((b) -> b.osService = osService);
        return builder;
    }

    @Override
    public QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        // TODO: osService is maybe not the best source of data. The stats method
        // is syncronized, which seems undesirable for the use case of node queries
        // which may happen 100 times a second. Maybe implement something
        // with volatile, or ReadWriteLock?
        OsStats.Cpu cpu = osService.stats().getCpu();
        return super.doRewrite(c -> c.test(cpu));
    }

    @EqualsAndHashCode(callSuper=true)
    @Getter
    static class DegradedCondition extends Condition {
        private final DegradedConditionType type;

        DegradedCondition(StreamInput in) throws IOException {
            super(in);
            type = DegradedConditionType.readFrom(in);
        }

        DegradedCondition(ConditionDefinition definition, DegradedConditionType type, int value, QueryBuilder query) {
            super(definition, value, query);
            this.type = Objects.requireNonNull(type);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            type.writeTo(out);
        }

        public boolean test(OsStats.Cpu cpu) {
            return test(type.extract(cpu));
        }

        void addXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TYPE.getPreferredName(), type);
        }
    }

    @FunctionalInterface
    private interface CpuStatExtractor {
        int extract(OsStats.Cpu cpu);
    }

    enum DegradedConditionType implements CpuStatExtractor, Writeable {
        // TODO: These should both be "regularized" such that the same constraint is valid on machines with 2 cpus and 48 cpus
        cpu(OsStats.Cpu::getPercent),
        load((cpu) -> (int) Math.round(cpu.getLoadAverage()[0]));

        private CpuStatExtractor extractor;

        DegradedConditionType(CpuStatExtractor extractor) {
            this.extractor = extractor;
        }

        public int extract(OsStats.Cpu cpu) {
            return extractor.extract(cpu);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        public static DegradedConditionType readFrom(StreamInput in) throws IOException {
            int ord = in.readVInt();
            if (ord < 0 || ord >= values().length) {
                throw new IOException("Unknown ConditionDefinition ordinal [" + ord + "]");
            }
            return values()[ord];
        }
    }

    private static class DegradedConditionParserState extends ConditionParserState<DegradedCondition> {
        private DegradedConditionType type;

        DegradedConditionParserState() {
            super(null);
            provider((def, value, query) -> new DegradedCondition(def, type, value, query));
        }

        public void setType(DegradedConditionType type) {
            this.type = type;
        }

        String checkValid() {
            if (type == null) {
                return "Missing field [type] in condition";
            }
            return super.checkValid();
        }
    }

    @VisibleForTesting
    void condition(ConditionDefinition def, DegradedConditionType type, int value, QueryBuilder query) {
        condition(new DegradedCondition(def, type, value, query));
    }
}
