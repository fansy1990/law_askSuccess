val t = Array(0,0,2,1,2,3,1,2)

val (t1,t2) =t.span(x => x !=1)

val tt = t.takeWhile(x => x!=1)
