def get_tape(filename):
    # Using the filename determine which tape the file is on
    # Here we make a BS tape id equal to the number of directory components in the filename
    return len(filename.split("/"))

def get_sequence(filename):
    # Using the filename, return an integer representing position on tape. It might be the sector, or just a sequence
    # Here we make up a BS sequence where we assume the sequence of files on tape is alphabeitcal on the last part of the basename
    return filename.split()[-1]

def get_file(filename):
    # Using the filename, request the file to be transfered to low latency storage
    return

def wait_file(filename):
    # Using the filename wait untill the file is on low latency storage
    if 'fail' in filename:
        raise Exception('fail in filename')
    return

def release_file(filename):
    # Using the filename tell the HSM that the file no longer needs to be on low latency storage (i.e. free up disk space)
    return
