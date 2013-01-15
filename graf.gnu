set datafile separator ","
set terminal png size 800,600
set output 'plot.png'

set multiplot

unset key

plot 'naiveBayes.csv' u 1:2 w l lw 3, \
     'naiveBayesMultinominal.csv' u 1:2 w l lw 3, \
     'perceptron.csv'u 1:2 w l lw 3 

set key; unset tics; unset border; unset xlabel; unset ylabel

plot [][0:1] 2 title 'NAIVE BAYES' lw 3, \
     2 title 'NAIVE BAYES MULTINOMINAL' lw 3, \
     2 title 'PERCEPTRON' lw 3
