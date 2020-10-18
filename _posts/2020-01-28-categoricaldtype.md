
# Using pandas category dtype for processing pipelines

When processing known data of a reasonable size in a ETL pipeline, often you want to eek out some performance for the reading and transformations. If you have mixed data types (e.g. strings and floats), doing standard operations can be costly given that pandas makes a best-guess at the data type and will often read in particular columns as objects. 

Specifying data types when you read in the data has many benefits, and is even necessary for other frameworks such as dask. But when you are working with strings, they can take up alot of memory and are slower to evaluate. 

Enter the "category" data type from pandas.

If you know beforehand that the values in a column are categorical, you can specify on read that the dtype of the column should be considered as such. 


```python
import pandas as pd
from pandas.api.types import CategoricalDtype
import numpy as np
```


```python
pd.DataFrame({'PRICE_MULTIPLE':np.random.randint(low=1,high=5,size=10000),
              'FX':np.random.choice(['USD','EUR','JPY','GBP'],size=10000),
              'PRICE':np.random.random(size=10000)
             }).to_csv('sample_data.csv',sep=';',index=False)
```


```python
df_withcat = pd.read_csv('sample_data.csv',sep=';',dtype={'PRICE_MULTIPLE':int,'FX':'category','PRICE':float})
df_nocat = pd.read_csv('sample_data.csv',sep=';',dtype={'PRICE_MULTIPLE':int,'FX':str,'PRICE':float})
```


```python
df_withcat.memory_usage()
```




    Index               128
    PRICE_MULTIPLE    40000
    FX                10192
    PRICE             80000
    dtype: int64




```python
df_nocat.memory_usage()
```




    Index               128
    PRICE_MULTIPLE    40000
    FX                80000
    PRICE             80000
    dtype: int64



## What do I gain from this?

As you can see, the memory footprint with the category data type is significantly smaller. For the toy example, it may not be noticable, but for datasets with many categorical variables it makes a large difference.

The real upside is realized when doing tranformations based on categorical data types instead of strings, however.


```python
%timeit -r 10 -n 1000 df_withcat.loc[:,'FX']=='USD'
%timeit -r 10 -n 1000 df_nocat.loc[:,'FX']=='USD'
```

    189 µs ± 13.9 µs per loop (mean ± std. dev. of 10 runs, 1000 loops each)
    900 µs ± 151 µs per loop (mean ± std. dev. of 10 runs, 1000 loops each)
    

For the simple boolean operation above, there is a __~80% decrease in runtime__. For complexer functions, this computation savings will be obviously much more significant.

What is the reason for this?

Under the hood, the different values of the categorical feature are encoded into a dictionary, and the actual series is then indexed with the values of the categories. Therefore operations on categorical variables are similar to operations with integers -- or even faster since they are encoded as `int8` instead of the default `int32`. This is a way of encoding the data more efficiently, which is broadly also how compression algorithms work. 


```python
%timeit -r 10 -n 1000 df_withcat.loc[:,'FX']=='USD'
%timeit -r 10 -n 1000 df_withcat.loc[:,'PRICE_MULTIPLE']==1
```

    195 µs ± 36.1 µs per loop (mean ± std. dev. of 10 runs, 1000 loops each)
    204 µs ± 12.8 µs per loop (mean ± std. dev. of 10 runs, 1000 loops each)
    


```python
df_withcat.loc[:,'FX'].head().cat.codes
```




    0    3
    1    2
    2    1
    3    1
    4    0
    dtype: int8



## What should you look out for when using the categorical data type?

The main thing to be careful of is when appending data to the dataframe or adding/altering categories that were not in the original categories. This will cause python to convert the categorical dtype (back) to a object dtype. This can sneak up on you in your pipeline and have unintended side effects, not the least of which is a deterioration in performance.

Let's try to change one of the values in the categorical column to a value not included in the original categories inferred from pandas.


```python
df_withcat2 = df_withcat.copy()
```


```python
try:
    df_withcat2.iloc[-1] = (8,"CAD",0.8)
except Exception as e:
    error_msg = e
error_msg
```




    ValueError('Cannot setitem on a Categorical with a new category, set the categories first')



As you can see, python throws an exception, complaining that it can't add a new category to an existing categorical variable. This, is a good thing. No one _likes_ exceptions, but it is better to know than not to know.

Another case is when you append data to an existing dataframe with a categorical variable column.

Appending a `DataFrame` which has the same categories works just fine.


```python
df_withcat3a = df_withcat.copy()
df_withcat3b = df_withcat.iloc[:50,:].copy()
df_withcat3_full = pd.concat([df_withcat3a,df_withcat3b]) # OK
df_withcat3_full.reset_index().FX.tail()
```




    10045    EUR
    10046    GBP
    10047    EUR
    10048    EUR
    10049    USD
    Name: FX, dtype: category
    Categories (4, object): [EUR, GBP, JPY, USD]



But if we try to append a `DataFrame` with a different dtype on the column, then pandas converts to `obj` dtype.


```python
df_withcat3c = df_nocat.iloc[:50,:].copy()
df_withcat3_full = pd.concat([df_withcat3a,df_withcat3c]) # Works, but drops Category
df_withcat3_full.reset_index().FX.tail()
```




    10045    EUR
    10046    GBP
    10047    EUR
    10048    EUR
    10049    USD
    Name: FX, dtype: object



What about a `DataFrame` with category dtype, but with a different category set?


```python
df_withcat3d = df_nocat.copy().assign(
    FX=np.random.choice(['USD','EUR','JPN','GBP','CAD'],size=10000))\
.astype({"FX":'category'})

df_withcat3_full = pd.concat([df_withcat3a,df_withcat3d]) # Works, but drops Category
df_withcat3_full.reset_index().FX.tail()
```




    19995    CAD
    19996    USD
    19997    GBP
    19998    JPN
    19999    CAD
    Name: FX, dtype: object



## So whats the best way to avoid these corners?

In my personal experience, nearly any column that I want to type as categorical also has a discrete number of categories that are known _a priori_. Therefore, I would typically write a dictionary for dtypes to be enforced "on read", and input all possible options in the category dtype beforehand. So even if the category "CAD" doesn't show up initially, python won't squawk if it shows up later.


```python
dt = {"FX": CategoricalDtype(categories=["USD","EUR","JPY","GBP","CAD"]),'PRICE_MULTIPLE':int,'PRICE':float}
df_withcat = pd.read_csv('sample_data.csv',sep=';',dtype=dt)
df_withcat.FX.tail()
```




    9995    GBP
    9996    USD
    9997    USD
    9998    USD
    9999    USD
    Name: FX, dtype: category
    Categories (5, object): [USD, EUR, JPY, GBP, CAD]




```python
df_withcat.iloc[-1] = (8,"CAD",0.8)
df_withcat.FX.tail()
```




    9995    GBP
    9996    USD
    9997    USD
    9998    USD
    9999    CAD
    Name: FX, dtype: category
    Categories (5, object): [USD, EUR, JPY, GBP, CAD]



This will help you preserve your types on read, but python being dynamically-typed, won't give you any guarantees. The best way to do this is with some kind of validation tool, such as great-expectations, pandera, or cerberus.

But this is a topic for another post.
