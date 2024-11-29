#include <db/ColumnStats.hpp>
#include <algorithm>
#include <stdexcept>

using namespace db;

// Constructor for ColumnStats
ColumnStats::ColumnStats(unsigned buckets, int min, int max)
    : buckets(buckets), min(min), max(max), histogram(buckets, 0), totalValues(0) {
    // Validate input arguments
    if (max < min || buckets == 0) {
        throw std::invalid_argument("Invalid arguments for ColumnStats");
    }
    // Calculate the bucket width, ensuring all values fit into the defined buckets
    bw = (max - min + buckets - 1) / buckets; // Adding (buckets - 1) ensures rounding up
}

// Add a value to the histogram
void ColumnStats::addValue(int v) {
    // Ignore values outside the [min, max] range
    if (v < min || v > max) {
        return;
    }
    // Determine the bucket index for the value
    unsigned bucketIndex = (v - min) / bw;
    // Ensure the bucket index does not exceed the range
    bucketIndex = std::min(bucketIndex, buckets - 1);
    // Increment the count for the corresponding bucket
    histogram[bucketIndex]++;
    // Increment the total count of values
    totalValues++;
}

// Estimate the cardinality (number of matching records) for a given predicate and value
size_t ColumnStats::estimateCardinality(PredicateOp op, int v) const {
    // Return 0 if there are no values in the histogram
    if (totalValues == 0) return 0;

    // Handle cases where v is outside the [min, max] range
    if (v < min) {
        switch (op) {
            case PredicateOp::GT:  // All values are greater
            case PredicateOp::GE:  // All values are greater or equal
            case PredicateOp::NE:  // All values are not equal
                return totalValues;
            default: // No values satisfy the predicate
                return 0;
        }
    }
    if (v > max) {
        switch (op) {
            case PredicateOp::LT:  // All values are less
            case PredicateOp::LE:  // All values are less or equal
            case PredicateOp::NE:  // All values are not equal
                return totalValues;
            default: // No values satisfy the predicate
                return 0;
        }
    }

    // Calculate the bucket index and position within the bucket for v
    int bucketIndex = (v - min) / bw;
    bucketIndex = std::clamp(bucketIndex, 0, static_cast<int>(buckets - 1)); // Ensure bucket index is valid
    int vInBucketIndex = (v - min) % bw; // Position of v within the bucket

    // Handle each predicate case
    switch (op) {
        case PredicateOp::EQ: {
            // Return the fraction of the bucket corresponding to the value
            return static_cast<size_t>(histogram[bucketIndex] / static_cast<double>(bw));
        }
        case PredicateOp::NE: {
            // Return all values except those equal to v
            return totalValues - static_cast<size_t>(histogram[bucketIndex] / static_cast<double>(bw));
        }
        case PredicateOp::LT: {
            // Count all values in buckets less than bucketIndex
            size_t count = 0;
            for (int i = 0; i < bucketIndex; ++i) count += histogram[i];
            // Add the fraction of the current bucket corresponding to values less than v
            double fraction = static_cast<double>(vInBucketIndex) / bw;
            count += static_cast<size_t>(histogram[bucketIndex] * fraction);
            return count;
        }
        case PredicateOp::LE: {
            // Count all values in buckets less than bucketIndex
            size_t count = 0;
            for (int i = 0; i < bucketIndex; ++i) count += histogram[i];
            // Add the fraction of the current bucket corresponding to values less than or equal to v
            double fraction = static_cast<double>(vInBucketIndex + 1) / bw;
            count += static_cast<size_t>(histogram[bucketIndex] * fraction);
            return count;
        }
        case PredicateOp::GT: {
            // Count the fraction of the current bucket corresponding to values greater than v
            size_t count = 0;
            double fraction = static_cast<double>(bw - vInBucketIndex - 1) / bw;
            count += static_cast<size_t>(histogram[bucketIndex] * fraction);
            // Add all values in buckets greater than bucketIndex
            for (int i = bucketIndex + 1; i < buckets; ++i) count += histogram[i];
            return count;
        }
        case PredicateOp::GE: {
            // Count the fraction of the current bucket corresponding to values greater than or equal to v
            size_t count = 0;
            double fraction = static_cast<double>(bw - vInBucketIndex) / bw;
            count += static_cast<size_t>(histogram[bucketIndex] * fraction);
            // Add all values in buckets greater than bucketIndex
            for (int i = bucketIndex + 1; i < buckets; ++i) count += histogram[i];
            return count;
        }
        default:
            // Throw an exception for unsupported predicates
            throw std::invalid_argument("Unsupported PredicateOp");
    }
}
