set datafile separator ","
set terminal postscript enhanced eps solid colour "Helvetica" 22
set output 'plot.eps'

set multiplot

unset key

plot 'naiveBayes.csv' u 1:2 w l lw 3, \
     'naiveBayesMultinominal.csv' u 1:2 w l lw 3, \
     'perceptron.csv'u 1:2 w l lw 3 

set key; unset tics; unset border; unset xlabel; unset ylabel

plot [][0:1] 2 title 'NAIVE BAYES' lw 3, \
     2 title 'NAIVE BAYES MULTINOMINAL' lw 3, \
     2 title 'PERCEPTRON' lw 3
