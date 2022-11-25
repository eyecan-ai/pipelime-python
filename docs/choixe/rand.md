# Rand Tutorial

## 1 - Basic Usage
How to get a random uniform value.

Random float between 0 and 1
```
$rand
```

Random float between 0 and N
```
$rand(3.5)
```

Random float between M and N
```
$rand(-2.0, 3.5)
```

## 2 - Integer Values
How to generate a discrete uniform value.

Integer arguments -> integer random
```
$rand(10)
```

```
$rand(-5, 10)
```

## 3 - Repeated Sampling
How to sample random lists and tensors.

If you set the `n` argument to a positive integer, you get a list of `n` values.
```
$rand(n=100000)
```

If  `n` is a list, you get a numpy array of shape `n`
```
$rand(n=[10, 100, 25])
```

All the previous rules apply, by default you get float lists/arrays, if you set the bounds to integer values you get integer lists/arrays.

## 4 - Custom PDFs

### 4.1 - Piecewise Constant

You can pass a list of floats to the `pdf` argument.
The min-max range will be partitioned into `len(pdf)` bins, and the values of `pdf` will be used as the probability of the bin. 

**Don't worry about the normalization**: the probabilities will be automatically normalized.

Here, the interval [0, 1] is partitioned into three parts:
- In [0, 1/3] probability is `1×p`
- In [1/3, 2/3] probability is `2×p`
- In [2/3, 1] probability is `0.15×p`
```
$rand(n=100000, pdf=[1.0, 2.0, 0.15])
```

Another example with range [-10.0, 10.0], partitioned into 5 parts with increasing probabilities:
```
$rand(-10.0, 10.0, n=100000, pdf=[1.0, 2.0, 3.0, 4.0, 5.0])
```

PDFs can be used also with discrete distributions, just set the bounds to integer values:
```
$rand(-10, 10, n=100000, pdf=[1.0, 2.0, 3.0, 4.0, 5.0])
```

### 4.2 - Piecewise Constant (custom intervals)

The `pdf` argument can also accept list of pairs. In each pair, the first element represents the point at which the probability expressed in the second element takes effect.

The following example works this way:
- at 0.0, set the probability of the first bin to 0.5
- at 0.3, set the probability of the second bin to 0.1
- at 0.6, set the probability of the third bin to 0.3

The bounds are inferred from the first and last element of the list, in this case, from 0 to 0.6
```
$rand(n=100000, pdf=[[0.0, 0.5], [0.3, 0.1], [0.6, 0.3]])
```

Use custom bounds if you don't want to extend past the last value
```
$rand(1.0, n=100000, pdf=[[0.0, 0.5], [0.3, 0.1], [0.6, 0.3]])
```

### 4.3 - Piecewise Linear

Define a piecewise linear by using a 2-element list as the probability value (works only with custom intervals)

The following example works this way:
- at 0.0, set the probability of the first bin to 1.0, descending linearly to 0.5 at the end of the bin
- at 0.3, set the probability of the second bin to 0.1, increasing linearly to 0.2 at the end of the bin
- at 0.6, set the probability of the third bin to 0.4, increasing linearly to 0.8 at the end of the bin
- at 1.0, set the probability to 0.0
```
$rand(n=100000, pdf=[[0.0, [1.0, 0.2]], [0.3, [0.1, 0.2]], [0.6, [0.4, 0.8]], [1.0, 0.0]])
```

### 4.4 - Lambdas

You can also use the `pdf` argument to define a custom distribution with a lambda expression
The lambda should have the following interface:
```
Callable[[np.ndarray], np.ndarray]
```

You can use np.xxx functions to define your distribution, it will be automatically imported 
in the available modules.
```
$rand(10.0, n=100000, pdf=lambda x: np.exp(-x))
```

### 4.5 - Mixed Piecewise

You can also mix all the previous pieces together to define a piecewise PDF.
The following example works this way:
- at 0.0, set the probability of the first bin to 1.0, descending linearly to 0.5 at the end of the bin
- at 0.3, set the probability of the second bin to a custom lambda
- at 0.6, set the probability of the third bin to 0.4
- at 1.0, set the probability to 0.0
```
$rand(n=100000, pdf=[[0.0, [1.0, 0.5]], [0.3, lambda x: np.exp(-10 * (x - 0.3))], [0.6, 0.4], [1.0, 0.0]])
```