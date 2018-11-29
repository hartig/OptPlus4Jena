package se.liu.ida.jenaext.optplus.sparql.engine.join;

import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.join.JoinKey;
import org.junit.Test;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterNestedLoopJoinPlusMaterializeRightFirstTest extends AbstractQueryIterJoinPlusTest
{
	@Override
	public QueryIterator createIterator( JoinKey joinKey,
                                         QueryIterator left,
                                         QueryIterator right,
                                         ExecutionContext execCxt )
	{
		return new QueryIterNestedLoopJoinPlusMaterializeRightFirst( left, right, execCxt );
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
