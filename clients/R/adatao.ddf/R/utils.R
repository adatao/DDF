
isInteger <- function(value) {
  value == round(value)
}

# trim whitespaces
# returns string w/o leading whitespace
trim.leading <- function (x)  sub("^\\s+", "", x)

# returns string w/o trailing whitespace
trim.trailing <- function (x) sub("\\s+$", "", x)

# returns string w/o leading or trailing whitespace
trim <- function (x) gsub("^\\s+|\\s+$", "", x)


# Function for looking up x's values in y and return positions
# Usually for converting column names to column indices
.lookup = function(x, y) {
  if (is.null(x) | is.null(y)) {
    stop("x and y must be not null")
  }
  pos <- match(x, y)
  if (any(is.na(pos))) {
    return((paste0("Can not find these column names: ",paste0(x[which(is.na(pos))],collapse=" "))))
  }  
  return(pos)
}