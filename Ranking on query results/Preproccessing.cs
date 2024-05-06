using System;
using System.Collections.Generic;
using System.Linq;

//The first program will do some preprocessing on the data and/or the workload.
//During this phase, a meta database will be constructed and filled.
//This metadb will be used when answering the actual queries.



public class Preprocessing
{
    /*
    CREATE TABLE autompg (
    id integer NOT NULL,
    mpg real,
    cylinders integer,
    displacement real,
    horsepower real,
    weight real,
    acceleration real,
    model_year integer,
    origin integer,
    brand text, 
    model text,
    type text,
    PRIMARY KEY (id)
    );
     */

    //fill metadb met posting lists
    //fill metadb met idf's = log(N/dft) rareness of a term, log(lengte van posting list / lengte van de file)
    //attribute similarity = sim(T,Q) = sum to m: (S(t,q),
    //          waar S(ti,qj) = J(W(t),W(q) * QF(q))
    //                  waar J(W(t),W(q)) = W(t) and W(q) / |W(t)| or |W(q)| =  en QF(q) = 1 + log(Fq) waar Fq = aantal keer dat q voorkomt in Q


    public Dictionary<string, float> RQF = new Dictionary<string, float>();
    public Dictionary<string, float[]> Database = new Dictionary<string, float[]>();

    public Dictionary<string, string[]> types = new Dictionary<string, string[]>();

    public float RGQFmax()
    {
        throw(new NotImplementedException());
        
    }

    //TODO fill RQF functie


    public float Sim(string t, string q)
    {
        float sum = 0;
        int m = q.Length;

        //sum to m: (S(t,q),
        for (int i = 0; i < m; i++)
        {
            sum += S(t[i], q[i]);
        }

        return sum;
    }

    public float QFSimularity(string u, string v)
    {
        if(u != v)
            return 0;

        return QF(u);
    }


    public float QF(string q)
    {
        throw(new NotImplementedException("RQF NOT FILLED YET"));
        //RQF(V) / RGQFmax
        return (float)RQF[q] / RGQFmax();

    }


    //S(ti,qj) = J(W(t),W(q) * QF(q)), 
    // 
    public float S(string t, string q)
    {
        return J(W(t), W(q)) * QFSimularity(q, t);
    }

    //subset van de queries in de workload waarin waarde v voorkomt in een in clause voor een specifiek attribuut
    public float[] W(string attribute)
    {
        //Dit is gewoon de postinglist van t

        return Database[attribute];
    }


    //| W(t)| and |W(q)| /  | W(t)| or |W(q)|
    //Jaquard coefficient meet de similarity tussen 2 sets
    public float SlowJ(float[] t, float[] q)
    {
        List<float> intersection = Intersect(t, q);
        List<float> union = Union(t, q);

        return intersection.Count / union.Count;
    }

    //set intersection
    public List<float> Intersect(float[] t, float[] q)
    {
        List<float> result = new List<float>();
        int i1 = 0;
        int i2 = 0;

        while (i1 < t.Length && i2 < q.Length)
        {

            if (t[i1] == q[i2])
            {
                result.Add(t[i1]);
                i1++;
                i2++;
            }
            else if (t[i1] < q[i2])
            {
                i1++;
            }
            else
            {
                i2++;
            }

        }

        return result;
    }
    //set union
    public List<float> Union(float[] t, float[] q)
    {
        //stop alle van t in de set 
        List<float> result = new List<float>(t);

        HashSet<float> set = new HashSet<float>(t);


        //en voeg de van q toe als ze er nog niet in zitten
        for (int i = 0; i < q.Length; i++)
        {
            if (!set.Contains(q[i]))
            {
                result.Add(q[i]);
                set.Add(q[i]);
            }
        }

        return result;
    }


    //length set intersection / length set union
    public float J(float[] t, float[] q)
    {
        return FastIntersect(t, q) / FastUnion(t, q);
    }
    //length set intersection
    public float FastIntersect(float[] t, float[] q)
    {
        int result = 0;
        int i1 = 0;
        int i2 = 0;

        while (i1 < t.Length && i2 < q.Length)
        {

            if (t[i1] == q[i2])
            {
                //update length
               result++;

                //update index
                i1++;
                i2++;
            }
            else if (t[i1] < q[i2])
            {
                i1++;
            }
            else
            {
                i2++;
            }

        }

        return result;
    }
    //length set union
    public float FastUnion(float[] t, float[] q)
    {
        //stop alle van t in de set 
        float result = t.Length;
        HashSet<float> set = new HashSet<float>(t);


        //en voeg de van q toe als ze er nog niet in zitten
        for (int i = 0; i < q.Length; i++)
        {
            if (!set.Contains(q[i]))
            {
                result++;
                set.Add(q[i]);
            }
        }

        return result;
    }


    public static void Main()
    {

        Console.WriteLine("Preprocessing");
    }
}
