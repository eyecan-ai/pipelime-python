if __name__ == "__main__":
    import sys

    numbers = []
    output_file, add_sharp = None, False

    # Parse the command line arguments
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == "--number":
            numbers.append(sys.argv[(i := i + 1)])
        elif sys.argv[i] == "--output":
            output_file = sys.argv[(i := i + 1)]
        elif sys.argv[i] == "--add_sharp":
            add_sharp = True
        else:
            raise ValueError(f"Unknown argument {sys.argv[i]}")
        i = i + 1

    assert output_file is not None

    # Open the output file in write mode
    prefix = "#" if add_sharp else ""
    with open(output_file, "w") as file:
        # Write each number to the file
        for number in numbers:
            file.write(prefix + number + "\n")
