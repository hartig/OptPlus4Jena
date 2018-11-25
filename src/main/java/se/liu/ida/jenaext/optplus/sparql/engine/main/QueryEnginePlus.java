package se.liu.ida.jenaext.optplus.sparql.engine.main;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.QueryEngineMain;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.Symbol;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryEnginePlus extends QueryEngineMain
{
	static final public Symbol useOptPlusSemantics      = Symbol.create("se.liu.ida.jenaext.optplus.sparql.engine.main.useOptPlusSemantics");
	static final public Symbol classnameOptPlusIterator = Symbol.create("se.liu.ida.jenaext.optplus.sparql.engine.main.classnameOptPlusIterator");
	
    static public void register()
    {
    	QueryEngineRegistry.addFactory(factory);
    }

    static public void unregister()
    {
        QueryEngineRegistry.removeFactory(factory);
    }


    public QueryEnginePlus(Op op, DatasetGraph dataset, Binding input, Context context)
    {
        super(op, dataset, input, context);
        registerOpExecutor();
    }

    public QueryEnginePlus(Query query, DatasetGraph dataset, Binding input, Context context)
    {
        super(query, dataset, input, context);
        registerOpExecutor();
    }

    private void registerOpExecutor()
    {
        QC.setFactory(context, OpExecutorPlus.factory);
    }


    static final private QueryEngineFactory factory = new QueryEngineFactory() {

    	@Override
        public boolean accept(Query query, DatasetGraph ds, Context cxt) {
            return true;
        }

    	@Override
        public boolean accept(Op op, DatasetGraph ds, Context cxt) {
            return true;
        }

    	@Override
        public Plan create(Query query, DatasetGraph dataset, Binding initialBinding, Context context) {
            final QueryEnginePlus engine = new QueryEnginePlus(query, dataset, initialBinding, context);
            return engine.getPlan();
        }

    	@Override
        public Plan create(Op op, DatasetGraph dataset, Binding initialBinding, Context context) {
            final QueryEnginePlus engine = new QueryEnginePlus(op, dataset, initialBinding, context);
            return engine.getPlan();
        }
    };

}
