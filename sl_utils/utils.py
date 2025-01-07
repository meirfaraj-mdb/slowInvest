import os


def convertTimeToHumanReadable(name, val, rounded=False):
    """
    Convert time values to a human-readable format based on the column name.
    Parameters:
    - name: str, the name of the column.
    - val: numeric, the time value to convert.
    - rounded: bool, whether to round the time components.
    Returns:
    - str, the converted time in a human-readable format.
    """
    nanos=0
    if name.endswith('Nanos'):
        nanos = val % 1_000
        val //= 1_000
        micros = val % 1_000
        val //= 1_000
        millis = val % 1_000
        val //= 1_000
        seconds = val % 60
        val //= 60
        minutes = val % 60
        val //= 60
        hours = val
    elif name.endswith('Micros'):
        micros = val % 1_000
        val //= 1_000
        millis = val % 1_000
        val //= 1_000
        seconds = val % 60
        val //= 60
        minutes = val % 60
        val //= 60
        hours = val
    elif name.endswith('Millis'):
        # Convert milliseconds to a detailed time format
        millis = val % 1_000
        val //= 1_000
        seconds = val % 60
        val //= 60
        minutes = val % 60
        val //= 60
        hours = val
        micros = 0
    else:
        return f"{val} (unknown unit)"
    if rounded:
        if micros >0 or millis > 0 or seconds > 0 or minutes > 0 or hours > 0:
            if nanos >= 500:
                micros +=1
            nanos = 0
            if millis > 0 or seconds > 0 or minutes > 0 or hours > 0:
                if micros >= 500:
                    millis += 1
                micros = 0
            if seconds > 0 or minutes > 0 or hours > 0:
                if millis >= 500:
                    seconds += 1
                millis = 0
                if minutes > 0 or hours > 0:
                    if seconds >= 30:
                        minutes += 1
                    seconds = 0
                    if minutes >= 60:
                       hours += minutes // 60
                       minutes = minutes % 60
    # Construct the human-readable time string
    time_str = ""
    if hours > 0:
        time_str += f"{int(hours)}H"
    if minutes > 0:
        time_str += f"{int(minutes)}min"
    if seconds > 0 and int(hours)==0:
        time_str += f"{int(seconds)}s"
    if millis > 0 and int(hours+minutes)==0:
        time_str += f"{int(millis)}ms"
    if micros > 0 and int(hours+minutes+seconds)==0:
        time_str += f"{int(micros)}micros"
    if nanos > 0 and int(hours+minutes+seconds+millis)==0:
        time_str += f"{int(nanos)}ns"
    return time_str or "0s"

def convertToHumanReadable(name, val, rounded=False):
    if name.endswith('_count'):
        return str(val)
    if name.endswith(('Millis','Micros','Nanos')):
        return str(convertTimeToHumanReadable(name, val, rounded))

    # Check if the name contains 'bytes' and convert to human-readable size
    if 'bytes' in name.lower():
        return convertBytesToHumanReadable(val)

    # Check if val is a list or tuple
    if isinstance(val, (list, tuple)):
        if not val:  # If the list or tuple is empty
            return ''
        elif len(val) == 1:  # If it contains a single element
            return str(val[0])
        else:  # If it contains multiple elements
            return ', '.join(map(str, val))

    # Check if val is a boolean
    if isinstance(val, (bool)):
        val = "True" if val else "False"

    # Check if val is a number and round it
    if isinstance(val, (int, float)):
        val = round(val)
    return str(val)

def convertBytesToHumanReadable(num_bytes):
    """
    Convert a byte value to a human-readable format (e.g., KB, MB, GB).
    """
    for unit in ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} YB"


def createDirs(directory_path):
    # Create the directory along with any necessary parent directories
    try:
        os.makedirs(directory_path, exist_ok=True)
    except Exception as e:
        print(f"An error occurred: {e}")


def remove_extension(file_path):
    root, _ = os.path.splitext(file_path)
    if root.endswith(".log"):
        root, _ = os.path.splitext(root)
    return root
