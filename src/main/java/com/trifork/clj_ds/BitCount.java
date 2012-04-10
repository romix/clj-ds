package com.trifork.clj_ds;

import java.util.Arrays;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

/* ==========================================================================
Bit Counting routines

Author: Gurmeet Singh Manku    (manku@cs.stanford.edu)
Java version: Roman Levenstein    (romixlev@yahoo.com)
Date:   27 Aug 2002
Date:    6 Aug 2011
========================================================================== */
public final class BitCount {
	private static final int fn_iterated_bitcount = 1;
	private static final int fn_sparse_ones_bitcount = 2;
	private static final int fn_dense_ones_bitcount = 3;
	private static final int fn_precomputed_bitcount = 4;
	private static final int fn_precomputed16_bitcount = 5;
	private static final int fn_parallel_bitcount = 6;
	private static final int fn_nifty_bitcount = 7;
	private static final int fn_nuonifty_bitcount = 8;
	private static final int fn_seander_bitcount = 9;
	private static final int fn_mit_bitcount = 10;
	private static final int fn_integer_bitcount = 11;

	private static final int SIZE_OF_INT_IN_BYTES = 4;
	private static double kbmin = 1.0e10, kbmax = 0.0, kbsum = 0.0, etsum = 0.0;
	
	/* Precomputed bitcount uses a precomputed array that stores the number of ones
	   in each char. */
	static char bits_in_char [] = new char[256] ;

	/* Here is another version of precomputed bitcount that uses a precomputed array
	   that stores the number of ones in each short. */

	static char[] bits_in_16bits = new  char [0x1 << 16] ;
	
	// DeBruijn sequence used to do very fast bits enumeration. Taken from 
	// http://stackoverflow.com/questions/838097/fastest-way-to-enumerate-through-turned-on-bits-of-an-integer
	final private static int[] MulDeBruijnBitPos = new int[] { 0, 1, 28, 2, 29,
				14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8, 31, 27, 13, 23, 21, 19,
				16, 7, 26, 12, 18, 6, 11, 5, 10, 9 };
	

	static {
	    compute_bits_in_char () ;
	    compute_bits_in_16bits () ;		
	}

	/* Iterated bitcount iterates over each bit. The while condition sometimes helps
	   terminates the loop earlier */
	static public int iterated_bitcount (int n)
	{
	    int count=0;
	    while (n != 0)
	    {
	        count += n & 0x1 ;
	        n >>>= 1 ;
	    }
	    return count ;
	}


	/* Sparse Ones runs proportional to the number of ones in n.
	   The line   n &= (n-1)   simply sets the last 1 bit in n to zero. */
	static public int sparse_ones_bitcount (int n)
	{
	    int count=0 ;
	    while (n!=0)
	    {
	        count++ ;
	        n &= (n - 1) ;
	    }
	    return count ;
	}


	/* Dense Ones runs proportional to the number of zeros in n.
	   It first toggles all bits in n, then diminishes count repeatedly */
	static public int dense_ones_bitcount (int n)
	{
	    int count = 8 * SIZE_OF_INT_IN_BYTES ;
	    n ^= (int) -1 ;
	    while (n!=0)
	    {
	        count-- ;
	        n &= (n - 1) ;
	    }
	    return count ;
	}


	static void compute_bits_in_char ()
	{
	    int i ;
	    for (i = 0; i < 256; i++)
	        bits_in_char [i] = (char)iterated_bitcount (i) ;
	    return ;
	}

	static public int precomputed_bitcount (int n)
	{
	    // works only for 32-bit ints

	    return bits_in_char [n         & 0xff]
	        +  bits_in_char [(n >>>  8) & 0xff]
	        +  bits_in_char [(n >>> 16) & 0xff]
	        +  bits_in_char [(n >>> 24) & 0xff] ;
	}



	static void compute_bits_in_16bits ()
	{
	    int i ;
	    for (i = 0; i < (0x1<<16); i++)
	        bits_in_16bits [i] = (char)iterated_bitcount (i) ;
	    return ;
	}

	static public int precomputed16_bitcount (int n)
	{
	    // works only for 32-bit int

	    return bits_in_16bits [n         & 0xffff]
	        +  bits_in_16bits [(n >>> 16) & 0xffff] ;
	}


	/* Parallel   Count   carries   out    bit   counting   in   a   parallel
	   fashion.   Consider   n   after    the   first   line   has   finished
	   executing. Imagine splitting n into  pairs of bits. Each pair contains
	   the <em>number of ones</em> in those two bit positions in the original
	   n.  After the second line has finished executing, each nibble contains
	   the  <em>number of  ones</em>  in  those four  bits  positions in  the
	   original n. Continuing  this for five iterations, the  64 bits contain
	   the  number  of ones  among  these  sixty-four  bit positions  in  the
	   original n. That is what we wanted to compute. */

	static int TWO(int c){
		return 0x1 << (c);
	}
	
	static int MASK(int c){
		return (int)((0xFFFFFFFFL) / (TWO(TWO(c)) + 1));
	}
	
	static int COUNT(int x, int c) {
		return ((x) & MASK(c)) + (((x) >>> (TWO(c))) & MASK(c));
	}

	static public int parallel_bitcount (int n)
	{
	    n = COUNT(n, 0) ;
	    n = COUNT(n, 1) ;
	    n = COUNT(n, 2) ;
	    n = COUNT(n, 3) ;
	    n = COUNT(n, 4) ;
	    /* n = COUNT(n, 5) ;    for 64-bit integers */
	    return n ;
	}


	/* Nifty  Parallel Count works  the same  way as  Parallel Count  for the
	   first three iterations. At the end  of the third line (just before the
	   return), each byte of n contains the number of ones in those eight bit
	   positions in  the original n. A  little thought then  explains why the
	   remainder modulo 255 works. */

	final static int MASK_01010101 = (int)(((0xFFFFFFFFL))/3);
	final static int MASK_00110011 = (int)(((0xFFFFFFFFL))/5);
	final static int MASK_00001111 = (int)(((0xFFFFFFFFL))/17);

	static public int nifty_bitcount (int n)
	{
	    n = (n & MASK_01010101) + ((n >>> 1) & MASK_01010101) ;
	    n = (n & MASK_00110011) + ((n >>> 2) & MASK_00110011) ;
	    n = (n & MASK_00001111) + ((n >>> 4) & MASK_00001111) ;
	    return n % 255 ;
	}

	/* NuoNifty was invented by Nuomnicron and is a minor variation on
	   the nifty parallel count to avoid the mod operation */
	final static int MASK_0101010101010101 = (int)((0xFFFFFFFFL)/3);
	final static int MASK_0011001100110011 = (int)((0xFFFFFFFFL)/5);
	final static int MASK_0000111100001111 = (int)((0xFFFFFFFFL)/17);
	final static int MASK_0000000011111111 = (int)((0xFFFFFFFFL)/257);
	final static int MASK_1111111111111111 = (int)((0xFFFFFFFFL)/65537);
	
	static public int nuonifty_bitcount (int n)
	{
	  n = (n & MASK_0101010101010101) + ((n >>> 1) & MASK_0101010101010101) ;
	  n = (n & MASK_0011001100110011) + ((n >>> 2) & MASK_0011001100110011) ;
	  n = (n & MASK_0000111100001111) + ((n >>> 4) & MASK_0000111100001111) ;
	  n = (n & MASK_0000000011111111) + ((n >>> 8) & MASK_0000000011111111) ;
	  n = (n & MASK_1111111111111111) + ((n >>> 16) & MASK_1111111111111111) ;

	  return n;
	}

	/* Seander parallel count takes only 12 operations, which is the same
	   as the lookup-table method, but avoids the memory and potential
	   cache misses of a table. It is a hybrid between the purely parallel
	   method above and the earlier methods using multiplies (in the
	   section on counting bits with 64-bit instructions), though it
	   doesn't use 64-bit instructions. The counts of bits set in the
	   bytes is done in parallel, and the sum total of the bits set in the
	   bytes is computed by multiplying by 0x1010101 and shifting right 24
	   bits.  From http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel */
	static public int seander_bitcount(int n)
	{
	  n = n - ((n >>> 1) & 0x55555555);	   		  // reuse input as temporary
	  n = (n & 0x33333333) + ((n >>> 2) & 0x33333333);	  // temp
	  return(((n + (n >>> 4) & 0xF0F0F0F) * 0x1010101) >>> 24); // count
	}

	/* MIT Bitcount

	   Consider a 3 bit number as being
	        4a+2b+c
	   if we shift it right 1 bit, we have
	        2a+b
	  subtracting this from the original gives
	        2a+b+c
	  if we shift the original 2 bits right we get
	        a
	  and so with another subtraction we have
	        a+b+c
	  which is the number of bits in the original number.

	  Suitable masking allows the sums of the octal digits in a 32 bit
	  number to appear in each octal digit.  This isn't much help unless
	  we can get all of them summed together.  This can be done by modulo
	  arithmetic (sum the digits in a number by molulo the base of the
	  number minus one) the old "casting out nines" trick they taught in
	  school before calculators were invented.  Now, using mod 7 wont help
	  us, because our number will very likely have more than 7 bits set.
	  So add the octal digits together to get base64 digits, and use
	  modulo 63.  (Those of you with 64 bit machines need to add 3 octal
	  digits together to get base512 digits, and use mod 511.)

	  This is HAKMEM 169, as used in X11 sources.
	  Source: MIT AI Lab memo, late 1970's.
	*/
	static public int mit_bitcount(int n)
	{
	    /* works for 32-bit numbers only */
	    int tmp;
	    tmp = n - ((n >>> 1) & 033333333333) - ((n >>> 2) & 011111111111);
	    return ((tmp + (tmp >>> 3)) & 030707070707) % 63;
	}


	static void verify_bitcounts (int x)
	{
	    int iterated_ones, sparse_ones, dense_ones ;
	    int precomputed_ones, precomputed16_ones ;
	    int parallel_ones, nifty_ones, seander_ones ;
	    int nuonifty_ones, mit_ones, integer_ones ;

	    iterated_ones      = iterated_bitcount      (x) ;
	    sparse_ones        = sparse_ones_bitcount   (x) ;
	    dense_ones         = dense_ones_bitcount    (x) ;
	    precomputed_ones   = precomputed_bitcount   (x) ;
	    precomputed16_ones = precomputed16_bitcount (x) ;
	    parallel_ones      = parallel_bitcount      (x) ;
	    nifty_ones         = nifty_bitcount         (x) ;
	    nuonifty_ones      = nuonifty_bitcount      (x) ;
	    seander_ones       = seander_bitcount       (x) ;
//	    mit_ones           = mit_bitcount           (x) ;
	    integer_ones       = Integer.bitCount       (x) ;

	    
//        System.out.printf ("Checking (0x%x)\n", x) ;
	    if (iterated_ones != sparse_ones)
	    {
	        System.out.printf ("ERROR: sparse_bitcount (0x%x) not okay!\n", x) ;
	        System.exit (0) ;
	    }

	    if (iterated_ones != dense_ones)
	    {
	    	System.out.printf ("ERROR: dense_bitcount (0x%x) not okay!\n", x) ;
	        System.exit (0) ;
	    }

	    if (iterated_ones != precomputed_ones)
	    {
	    	System.out.printf ("ERROR: precomputed_bitcount (0x%x) not okay!\n", x) ;
	        System.exit (0) ;
	    }

	    if (iterated_ones != precomputed16_ones)
	    {
	    	System.out.printf ("ERROR: precomputed16_bitcount (0x%x) not okay!\n", x) ;
	        System.exit (0) ;
	    }

	    if (iterated_ones != parallel_ones)
	    {
	    	System.out.printf ("ERROR: parallel_bitcount (0x%x) not okay!\n", x) ;
	        System.exit (0) ;
	    }

	    if (iterated_ones != nifty_ones)
	    {
	    	System.out.printf ("ERROR: nifty_bitcount (0x%x) not okay!\n", x) ;
	        System.exit (0) ;
	    }

	    if (iterated_ones != nuonifty_ones)
	    {
	    	System.out.printf ("ERROR: nuonifty_bitcount (0x%x) not okay!\n", x) ;
	        System.exit (0) ;
	    }

	    if (iterated_ones != seander_ones)
	    {
	    	System.out.printf ("ERROR: nifty_bitcount (0x%x) not okay!\n", x) ;
	        System.exit (0) ;
	    }

	    if (iterated_ones != integer_ones)
	    {
	    	System.out.printf ("ERROR: Inetger.bitcount (0x%x) not okay!\n", x) ;
	        System.exit (0) ;
	    }

//	    if (mit_ones != nifty_ones)
//	    {
//	    	System.out.printf ("ERROR: mit_bitcount (0x%x) not okay!\n", x) ;
//	        System.exit (0) ;
//	    }


	    return ;
	}

	static void bitspeeds()
	{
	  timeone("iterated", fn_iterated_bitcount);
	  timeone("sparse", fn_sparse_ones_bitcount);
	  timeone("dense", fn_dense_ones_bitcount);
	  timeone("precomputed", fn_precomputed_bitcount);
	  timeone("precomputed16", fn_precomputed16_bitcount);
	  timeone("parallel", fn_parallel_bitcount);
	  timeone("nifty", fn_nifty_bitcount);
	  timeone("nuonifty", fn_nuonifty_bitcount);
	  timeone("seander", fn_seander_bitcount);
	  timeone("mit", fn_mit_bitcount);
	  timeone("integer_bitcount", fn_integer_bitcount);
	}

	public static int res;
	static final int numtestsM = 1000;
//	static final int numtestsM = 100;
	static final int numtests = numtestsM * 1000000;
	
	static void time_nuonifty_bitcount() {
		  for(int i=numtests; i > 0; i--)
		  {
			  res = nuonifty_bitcount(i);
		  }		
	}
	
	static void timeone(String tname, int fn)
	{
	  
	  long start = System.nanoTime();

	  switch(fn) {
	  case fn_nuonifty_bitcount: 
		  time_nuonifty_bitcount();
		  break;
	  case fn_dense_ones_bitcount:
		  time_dense_ones_bitcount();
		  break;
	  case fn_mit_bitcount:
		  time_mit_bitcount();
		  break;
	  case fn_nifty_bitcount:
		  time_nifty_bitcount();
		  break;
	  case fn_parallel_bitcount:
		  time_parallel_bitcount();
		  break;
	  case fn_sparse_ones_bitcount:
		  time_sparse_ones_bitcount();
		  break;
	  case fn_iterated_bitcount:
		  time_iterated_bitcount();
		  break;
	  case fn_seander_bitcount:
		  time_saender_bitcount();
		  break;
	  case fn_precomputed16_bitcount:
		  time_precomputed16_bitcount();
		  break;
	  case fn_precomputed_bitcount:
		  time_precomputed_bitcount();
		  break;
	  case fn_integer_bitcount:
		  time_integer_bitcount();
		  break;
	  }
	  
	  long end = System.nanoTime();
	  
	  long timeDiff = end - start;
	  timeDiff = TimeUnit.MILLISECONDS.convert(timeDiff, TimeUnit.NANOSECONDS);

	  System.out.printf("%13s: %d million counts in %5d ms for %11.0f cnts/ms\n", tname, numtestsM, timeDiff, ((double)numtests)/((double)timeDiff));
	}


	private static void time_integer_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = Integer.bitCount(i);
		  }
	}


	private static void time_precomputed_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = precomputed_bitcount(i);
		  }
	}


	private static void time_precomputed16_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = precomputed16_bitcount(i);
		  }
	}


	private static void time_saender_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = seander_bitcount(i);
		  }
	}


	private static void time_iterated_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = iterated_bitcount(i);
		  }
	}


	private static void time_sparse_ones_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = sparse_ones_bitcount(i);
		  }
	}


	private static void time_parallel_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = parallel_bitcount(i);
		  }
	}


	private static void time_nifty_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = nifty_bitcount(i);
		  }
	}


	private static void time_mit_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = mit_bitcount(i);
		  }
	}


	private static void time_dense_ones_bitcount() {
		for(int i=numtests; i > 0; i--)
		  {
			  res = dense_ones_bitcount(i);
		  }
	}


	public static void main (String[] args)
	{
	    int i ;


	    verify_bitcounts (0xFFFFFFFF) ;
	    verify_bitcounts (0) ;

	    for (i = 0 ; i < 1000000 ; i++) {
	    	int value = (int)Math.round(Math.random()*Integer.MAX_VALUE);
	        verify_bitcounts (value) ;
	        verify_bitenumerations(value);
	    }

	    System.out.printf ("All BitCounts seem okay!  Starting speed trials\n") ;

	    bitspeeds();

	}
	


	/***
	 * parallel bitcount seems to be the most efficient for java
	 * @param n
	 * @return
	 */
	static public int bitCount1(int n)
	{
	    n = COUNT(n, 0) ;
	    n = COUNT(n, 1) ;
	    n = COUNT(n, 2) ;
	    n = COUNT(n, 3) ;
	    n = COUNT(n, 4) ;
	    /* n = COUNT(n, 5) ;    for 64-bit integers */
	    return n ;
	}
	
	static public int bitCount(int n)
	{
	    return seander_bitcount(n) ;
	}
	
	static Vector<Integer> enumerateBits(int value)
	{
	    Vector<Integer> data = new Vector<Integer>();

	    while (value != 0)
	    {
	        int m = (value & (- value));
	        value ^= m;
	        data.add(MulDeBruijnBitPos[(int)((m * 0x077CB531) >>> 27)]);
	    }

	    return data;
	}
	
	static Vector<Integer> enumerateBitsSlow(int value)
	{
	    Vector<Integer> data = new Vector<Integer>();

	    for (int i = 0; i < 32; i++)
	    {
	        if (((value >> i) & 1) == 1)
	        {
	            data.add(i);
	        }
	    }

	    return data;
	}

	private static void verify_bitenumerations(int value) {
		Vector<Integer> data1 = enumerateBits(value);
		Vector<Integer> data2 = enumerateBitsSlow(value);
		if(!data1.equals(data2)) {
	    	System.out.printf ("ERROR: verify_bitenumerations (0x%x) not okay!\n", value) ;
	        System.exit (0) ;			
		} else {
	    	System.out.printf ("ERROR: verify_bitenumerations (0x%x) are okay! bit set(%d) are: %s \n", value, data1.size(), Arrays.toString(data1.toArray(new Integer[0]))) ;			
		}
	}
}
