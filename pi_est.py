
import random
from pyspark import SparkContext

NUM_SAMPLES = 100000000

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

if __name__ == "__main__":
    
    sc = SparkContext('local', 'pi_est')

    count = sc.parallelize(range(0,NUM_SAMPLES)).filter(inside).count()
    print ("Pi estimate: %2.6f \n" %(4 * count / float(NUM_SAMPLES)))