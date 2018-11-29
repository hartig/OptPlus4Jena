package se.liu.ida.jenaext.optplus.sparql.engine.join;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.engine.join.JoinKey;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public abstract class AbstractQueryIterJoinPlusTest
{
	abstract public QueryIterator createIterator( JoinKey joinKey,
                                                  QueryIterator left,
                                                  QueryIterator right,
                                                  ExecutionContext execCxt );

	protected void noJoinPartner()
	{
		final ExecutionContext eCtxt = new ExecutionContext( ARQ.getContext(), null, null, null );

		final Var v1 = Var.alloc("v1");
		final Var v2 = Var.alloc("v2");
		final Var v3 = Var.alloc("v3");

		final BindingHashMap leftMapping = new BindingHashMap();
		leftMapping.add( v1, NodeFactory.createLiteral("x") );
		leftMapping.add( v2, NodeFactory.createLiteral("y") );

		final BindingHashMap rightMapping = new BindingHashMap();
		rightMapping.add( v1, NodeFactory.createLiteral("not x") );
		rightMapping.add( v3, NodeFactory.createLiteral("z") );

		final QueryIterator left  = QueryIterSingleton.create(leftMapping,eCtxt);
		final QueryIterator right = QueryIterSingleton.create(rightMapping,eCtxt);

		final JoinKey joinKey = JoinKey.create(v1);

		final QueryIterator test = createIterator(joinKey, left, right, eCtxt);

		assertTrue( test.hasNext() );
		assertEquals( leftMapping, test.next() );

		assertFalse( test.hasNext() );

		test.close();
	}

	protected void oneJoinPartner()
	{
		final ExecutionContext eCtxt = new ExecutionContext( ARQ.getContext(), null, null, null );

		final Var v1 = Var.alloc("v1");
		final Var v2 = Var.alloc("v2");
		final Var v3 = Var.alloc("v3");

		final BindingHashMap leftMapping = new BindingHashMap();
		leftMapping.add( v1, NodeFactory.createLiteral("x") );
		leftMapping.add( v2, NodeFactory.createLiteral("y") );

		final BindingHashMap rightMapping = new BindingHashMap();
		rightMapping.add( v1, NodeFactory.createLiteral("x") );
		rightMapping.add( v3, NodeFactory.createLiteral("z") );

		final QueryIterator left  = QueryIterSingleton.create(leftMapping,eCtxt);
		final QueryIterator right = QueryIterSingleton.create(rightMapping,eCtxt);

		final JoinKey joinKey = JoinKey.create(v1);

		final QueryIterator test = createIterator(joinKey, left, right, eCtxt);

		assertTrue( test.hasNext() );
		assertEquals( leftMapping, test.next() );

		assertTrue( test.hasNext() );

		final Binding m = test.next();
		assertTrue( m.contains(v1) );
		assertTrue( m.contains(v2) );
		assertTrue( m.contains(v3) );
		assertEquals( "x", m.get(v1).getLiteral().getLexicalForm() );
		assertEquals( "y", m.get(v2).getLiteral().getLexicalForm() );
		assertEquals( "z", m.get(v3).getLiteral().getLexicalForm() );

		test.close();
	}

}
