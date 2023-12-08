import pyspark
import random
NUM_SAMPLES=10000000
if not 'sc' in globals():
    sc = pyspark.SparkContext()
NUM_SAMPLES = 1000
def sample(p):
    x,y = random.random(),random.random()
    return 1 if x*x + y*y < 1 else 0
count = sc.parallelize(range(0, NUM_SAMPLES)) \
            .map(sample) \
            .reduce(lambda a, b: a + b)
result = 4.0 * count / NUM_SAMPLES
print (f"Pi is roughly {result}")
