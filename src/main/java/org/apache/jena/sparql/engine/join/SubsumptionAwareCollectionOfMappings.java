package org.apache.jena.sparql.engine.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.jena.atlas.iterator.IteratorConcat;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class SubsumptionAwareCollectionOfMappings
{
	protected final List<Binding>               noKeyBucket = new ArrayList<>();
	protected final Map<Object,  List<Binding>> buckets = new HashMap<>();
	protected final Map<Binding, List<Binding>> subsumingMappings = new HashMap<>();

	protected JoinKey joinKey = null;

	public void add( Binding mapping )
	{
		if ( joinKey == null )
			joinKey = createJoinKey(mapping);

		final Object longHash = JoinLib.hash(joinKey, mapping);
        if ( longHash == JoinLib.noKeyHash )
        	noKeyBucket.add(mapping);

        final List<Binding> bucket = buckets.remove(longHash);
        if ( bucket == null ) {
        	final List<Binding> newBucket = new ArrayList<>();
        	newBucket.add(mapping);
        	buckets.put(longHash, newBucket);
        	return;
        }

        boolean inputMappingAdded = false;
        final List<Binding> newBucket = new ArrayList<>(); 
        final Iterator<Binding> it = bucket.iterator();
        while ( it.hasNext() )
        {
        	final Binding earlierMapping = it.next();
        	final boolean earlierMappingSubsumed = subsumedBy(earlierMapping, mapping);
        	final boolean inputMappingSubsumed   = subsumedBy(mapping, earlierMapping);

        	if ( ! inputMappingAdded && earlierMappingSubsumed )
        	{
        		// Note, this case includes the case in which the two mappings
        		// are equivalent; i.e., inputMappingSubsumed may also be true
        		// and, thus, we have a duplicate.

        		newBucket.add(earlierMapping);

        		List<Binding> mappingsSubsumingEarlierMapping = subsumingMappings.get(earlierMapping);

        		if ( mappingsSubsumingEarlierMapping == null ) {
        			mappingsSubsumingEarlierMapping = new ArrayList<>();
        			subsumingMappings.put(earlierMapping, mappingsSubsumingEarlierMapping);
        		}
        			
        		mappingsSubsumingEarlierMapping.add(mapping);

        		inputMappingAdded = true;
        	}
        	else if ( ! inputMappingAdded && inputMappingSubsumed )
        	{
        		newBucket.add(mapping);

        		List<Binding> mappingsSubsumingEarlierMapping = subsumingMappings.remove(earlierMapping);
        		if ( mappingsSubsumingEarlierMapping == null )
        			mappingsSubsumingEarlierMapping = new ArrayList<>();

        		mappingsSubsumingEarlierMapping.add(earlierMapping);

        		subsumingMappings.put(mapping, mappingsSubsumingEarlierMapping);

        		inputMappingAdded = true;
        	}
        	else
        	{
        		newBucket.add(earlierMapping);
        	}
        }

        if ( ! inputMappingAdded )
    		newBucket.add(mapping);

        buckets.put(longHash, newBucket);
	}

	public Iterator<Binding> noKeyMappings()
	{
if ( noKeyBucket.size() > 0 )
System.out.println( "noKeyMappings " + noKeyBucket.size() );
		return noKeyBucket.iterator();
	}

	public Iterator<Binding> subsumedMappings()
	{
		final IteratorConcat<Binding> it = new IteratorConcat<>();

		final Iterator<List<Binding>> itBucket = buckets.values().iterator();
		while ( itBucket.hasNext() )
			it.add( itBucket.next().iterator() );

		return it;
	}

	public List<Binding> subsumingMappings( Binding m )
	{
		final List<Binding> mappingsSubsumingInputMapping = subsumingMappings.get( m );
		if ( mappingsSubsumingInputMapping != null )
{
System.out.println( "subsumingMappings " + mappingsSubsumingInputMapping.size() );
			return mappingsSubsumingInputMapping;
}

		return new ArrayList<>();
	}

	static public JoinKey createJoinKey( Binding mapping )
	{
		final JoinKey.Builder builder = new JoinKey.Builder();

		final Iterator<Var> it = mapping.vars();
		while ( it.hasNext() )
			builder.add( it.next() );

		return builder.build();
	}

	/**
	 * Return true if the first mappings is subsumed by the second mapping.
	 */
	static public boolean subsumedBy( Binding m1, Binding m2 )
	{
		final Iterator<Var> it = m1.vars();
		while ( it.hasNext() )
        {
            final Var v = it.next();
            final Node n1 = m1.get(v);
            final Node n2 = m2.get(v);

            if ( n2 == null || n2.equals(n1) )
            	return false;
        }

        return true;
	}

}
