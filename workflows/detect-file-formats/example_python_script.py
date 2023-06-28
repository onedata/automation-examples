# Python program for implementation of heap Sort

# To heapify subtree rooted at index i.
# n is size of heap


def heapify(argument, number, iteration):
    largest = iteration  # Initialize largest as root
    l = 2 * iteration + 1  # left = 2*i + 1
    r = 2 * iteration + 2  # right = 2*i + 2

    # See if left child of root exists and is
    # greater than root
    if l < number and argument[largest] < argument[l]:
        largest = l

    # See if right child of root exists and is
    # greater than root
    if r < number and argument[largest] < argument[r]:
        largest = r

    # Change root, if needed
    if largest != iteration:
        argument[iteration], argument[largest] = (
            argument[largest],
            argument[iteration],
        )  # swap

        # Heapify the root.
        heapify(argument, number, largest)


# The main function to sort an array of given size


def heapSort(argument_):
    number_ = len(argument_)

    # Build a maxheap.
    for iteration in range(number_ // 2 - 1, -1, -1):
        heapify(argument_, number_, iteration)

    # One by one extract elements
    for iteration in range(number_ - 1, 0, -1):
        argument_[iteration], argument_[0] = argument_[0], argument_[iteration]  # swap
        heapify(argument_, iteration, 0)


# Driver's code
if __name__ == "__main__":
    argument__ = [12, 11, 13, 5, 6, 7]

    # Function call
    heapSort(argument__)
    number__ = len(argument__)

    print("Sorted array is")
    for i in range(number__):
        print(f"{i}" % argument__[i], end=" ")
# This code is contributed by Mohit Kumra
