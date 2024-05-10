using System;
using System.Collections.Generic;



//The second program will be able to process queries and show the answers,
//making use of the metadb for ranking.
//Note that your metadb should be constructed only once,
//for this particular contents of the db and before processing a batch of queries.



/*
 *(Her)definities
   Gegeven attribuut Ak
   Voor elke waarde v uit dom(Ak) is Fk(v) het aantal tuples in R met Ak = v
   Laat N = |R| ;
   we definiëren idfk v) = log(N/Fk(v))
 *
 */

 namespace project;

public static class Program
{
    public static void Main()
    {
        Preprocessing p = new Preprocessing();
        p.Processing();


    }
}