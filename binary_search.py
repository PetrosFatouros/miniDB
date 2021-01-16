#-->Returns position of 'search_value', return -1 if 'search_value' does not exist in list
def _binary_search(list, search_value):
    low = 0
    high = len(list) - 1
    mid = 0
    while low <= high:
        mid = (high + low) // 2
        if list[mid] < search_value:
            low = mid + 1
        elif list[mid] > search_value:
            high = mid - 1
        else:
            return mid
    return -1

#-->If 'search_value' exists return its position and True, else return its supposed position and False
def _binary_search_v2(list, search_value):
    left = 0
    right = len(list)

    while left < right:
        mid = (left + right) // 2
        mid_element = list[mid]
        if search_value < mid_element:
            right = mid
        elif search_value > mid_element:
            left = mid + 1
        else:
            return mid, True
    return left, False
