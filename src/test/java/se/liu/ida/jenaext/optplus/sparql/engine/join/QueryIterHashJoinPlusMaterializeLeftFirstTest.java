package se.liu.ida.jenaext.optplus.sparql.engine.join;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.join.QueryIterHashJoinPlusMaterializeLeftFirst;
import org.junit.Test;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterHashJoinPlusMaterializeLeftFirstTest extends AbstractQueryIterJoinPlusTest
{
	@Override
	public QueryIterator createIterator( QueryIterator left,
                                         QueryIterator right,
                                         ExecutionContext execCxt )
	{
		return new QueryIterHashJoinPlusMaterializeLeftFirst( null, left, right, execCxt );
	}

	@Test
	@Override
	public void noJoinPartner()
	{
		super.noJoinPartner();
	}

	@Test
	@Override
	public void oneJoinPartner()
	{
		super.oneJoinPartner();
	}

}
