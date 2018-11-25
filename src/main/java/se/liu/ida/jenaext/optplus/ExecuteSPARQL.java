package se.liu.ida.jenaext.optplus;

import org.apache.jena.query.ARQ;

import se.liu.ida.jenaext.optplus.sparql.engine.main.QueryEnginePlus;
import arq.query;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class ExecuteSPARQL extends query
{
    public static void main( String... argv )
    {
        new ExecuteSPARQL(argv).mainRun();
    }

    public ExecuteSPARQL( String[] argv )
    {
    	super(argv);
    }

    @Override
    protected void processModulesAndArgs()
    {
    	super.processModulesAndArgs();
    }

    @Override
    protected void exec()
    {
    	ARQ.getContext().setTrue(QueryEnginePlus.useOptPlusSemantics);
    	ARQ.getContext().set(QueryEnginePlus.classnameOptPlusIterator, "QueryIterHashJoinPlusMaterializeLeftOnTheFly" );
        QueryEnginePlus.register();
    	super.exec();
    }

}
