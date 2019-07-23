package cmd

// As we discussed, the general architecture is that this is going to search a list of buckets and spawn s3Traversers for each bucket.
// This will modify the storedObject format a slight bit to add a "container" parameter.
