package org.apache.jena.sparql.engine.join;

import java.util.Iterator;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.join.AbstractIterHashJoin;
import org.apache.jena.sparql.engine.join.JoinKey;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterHashJoinPlusMaterializeLeftFirst extends AbstractIterHashJoin
{
	protected Iterator<Binding> itHashedMappings;

    public QueryIterHashJoinPlusMaterializeLeftFirst( JoinKey joinKey,
                                  QueryIterator left,
                                  QueryIterator right,
                                  ExecutionContext execCxt )
    {
        super(joinKey, left, right, execCxt);

        itHashedMappings = hashTable.values();
    }

    @Override
    protected boolean hasNextBinding()
    {
    	if ( itHashedMappings != null )
    	{
    		if ( itHashedMappings.hasNext() )
    			return true;

    		itHashedMappings = null;
    	}

    	return super.hasNextBinding();
    }

    @Override
    protected Binding moveToNextBinding()
    {
    	if ( itHashedMappings != null ) {
    		s_countResults++;
    		return itHashedMappings.next();
    	}
    	else {
    		return super.moveToNextBinding();
    	}
    }

    @Override
    protected Binding yieldOneResult( Binding rowCurrentProbe, Binding rowStream, Binding rowResult )
    {
    	return rowResult;
    }

    @Override
    protected Binding noYieldedRows( Binding rowCurrentProbe )
    {
        return null;
    }
    
    @Override
    protected QueryIterator joinFinished()
    {
        return null;
    }

}
