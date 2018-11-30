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
        if ( longHash == JoinLib.noKeyHash ) {
        	noKeyBucket.add(mapping);
        	return;
        }

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

        	if ( ! inputMappingAdded && subsumedBy(earlierMapping, mapping) )
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
        	else if ( ! inputMappingAdded && subsumedBy(mapping, earlierMapping) )
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
			return mappingsSubsumingInputMapping;

		return new ArrayList<>();
	}

	/**
	 * For debugging purposes.
	 */
	public void printStats()
	{
		System.out.println( "noKeyBucket.size(): " + noKeyBucket.size() );

		int cntSubsumedMappings = 0;
		int maxBucketSize = 0;
		int minBucketSize = Integer.MAX_VALUE;

		final Iterator<List<Binding>> itBucket = buckets.values().iterator();
		while ( itBucket.hasNext() ) {
			int bucketSize = itBucket.next().size();
			cntSubsumedMappings += bucketSize;
			if ( bucketSize > maxBucketSize )
				maxBucketSize = bucketSize;
			if ( bucketSize < minBucketSize )
				minBucketSize = bucketSize;
		}

		System.out.println( "buckets.size():      " + buckets.size() );
		System.out.println( "cntSubsumedMappings: " + cntSubsumedMappings );
		System.out.println( "average bucket size: " + cntSubsumedMappings/(0d+buckets.size()) );
		System.out.println( "minimum bucket size: " + minBucketSize );
		System.out.println( "maximum bucket size: " + maxBucketSize );

		int cntSubsumingMappings = 0;
		int maxSubsumingMappings = 0;
		int minSubsumingMappings = Integer.MAX_VALUE;
		final Iterator<List<Binding>> it2 = subsumingMappings.values().iterator();
		while ( it2.hasNext() ) {
			int size = it2.next().size();
			cntSubsumingMappings += size;
			if ( size > maxSubsumingMappings )
				maxSubsumingMappings = size;
			if ( size < minSubsumingMappings )
				minSubsumingMappings = size;
		}

		System.out.println( "subsumingMappings.size():  " + subsumingMappings.size() );
		System.out.println( "cntSubsumingMappings: " + cntSubsumingMappings );
		System.out.println( "average number of subsuming mappings: " + cntSubsumingMappings/(0d+subsumingMappings.size()) );
		System.out.println( "minimum number of subsuming mappings: " + minSubsumingMappings );
		System.out.println( "maximum number of subsuming mappings: " + maxSubsumingMappings );
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

            if ( n2 == null || ! n2.equals(n1) )
            	return false;
        }

        return true;
	}

}
