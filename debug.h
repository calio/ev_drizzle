#ifndef __DEBUG_H__
#define __DEBUG_H__

#if defined(_DEBUG_) && (_DEBUG_)

#define dbgin(...) fprintf(stderr, "enter function [%s]\n", __func__);

#else

#define dbgin()

#endif

#endif
