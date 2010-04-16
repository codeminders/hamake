-- $infile  - build/test/class-size-histogram
-- $outfile - build/test/class-size-median

A = load '$infile' using PigStorage() as (bin:int, count:int);
B = foreach A generate *;
C = cross A, B;
D = group C by $0;
E = foreach D generate group, FLATTEN($1.A::count), FLATTEN($1.B::count);
F = DISTINCT E;
G = foreach F generate $0, ($1 == $2 ? 0 : ($1 > $2 ? 1 : -1));
H = group G by group;
I = foreach H generate $0, SUM(G.$1);
J = foreach I generate $0, ($1 > 0 ? $1 : -1*$1);
K = order J by $1;
L = LIMIT K 1;
M = foreach L generate $0;
store M into '$outfile';