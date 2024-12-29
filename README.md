## PySpark RDD Tasks

### Task 1: ReduceByKey Transformation on a Pair RDD
1. Create a pair RDD named `Rdd` with the tuples `(1,2)`, `(3,4)`, `(3,6)`, `(4,5)`.
2. Transform the `Rdd` using `reduceByKey()` into a pair RDD `Rdd_Reduced` by adding the values with the same key.
3. Collect the contents of `Rdd_Reduced` and iterate to print the output.
4. Output should look like this:
   ```
   Key 1 has 2 Counts
   Key 3 has 10 Counts
   Key 4 has 5 Counts
   ```

---

### Task 2: Word Count in an Input File
1. Read the input file using PySpark and create an RDD.
2. Count the occurrence of each word in the file.
3. [Hint]: Use `flatMap()` to split the lines into words and `map()` to transform each word into a key-value pair of `(word, 1)`.

---

### Task 3: Count Unique Keys in a Key-Value Pair RDD
1. You have a key-value pair RDD similar to Task 1.
2. Count the unique keys and assign the result to a variable `total`.
3. Identify the type of `total`.
4. Iterate over `total` and print the keys and their counts.
5. Output should look like this:

---

### Task 4: Word Frequency in a Text Document
1. The volume of unstructured data (log lines, images, binary files) is growing dramatically. PySpark is an excellent framework for analyzing this type of data through RDDs.
2. In this task, analyze the frequency of words in a text document using PySpark. The text document provided is `Complete_Shakespeare.txt`.

####Steps:
- Create a base RDD from the `Complete_Shakespeare.txt` file.
- Use RDD transformation to create a long list of words from each element of the base RDD.
- Count the total words in `splitRDD`.
- Remove stop words from your data. Convert the words in `splitRDD` to lowercase and then remove stop words from the `stop_words` curated list.
- Create a pair RDD where each element is a pair tuple of `('word', 1)`.
- Get the count of the number of occurrences of each word (word frequency) in the pair RDD.
- Group the elements of the pair RDD by key (word) and add up their values.
- Swap the keys (word) and values (counts) so that keys are counts and values are words.
- Finally, sort the RDD in descending order and print the 10 most frequent words and their frequencies.
