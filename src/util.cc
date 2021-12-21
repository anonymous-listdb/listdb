#include "util.h"

void fprintf_with_file_and_line(const char* file, const int line, FILE* strm, const char* fmt, ...) {
  char buf[100];
  va_list ap;
  va_start(ap, fmt);
  vsprintf(buf, fmt, ap);
  va_end(ap);
  fprintf(strm, "%s (%s:%d)\n", buf, file, line);
}
