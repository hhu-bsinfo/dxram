// compile:
// gcc -c JNIconsole.c -o JNIconsole.o -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include
//
// link (uses static version of readline lib)
// g++ -dynamiclib -undefined suppress -flat_namespace *.o -o libJNIconsole.dylib libreadline.a -ltermcap

#include <jni.h>
#include <stdlib.h>

#if defined (HAVE_CONFIG_H)
#include <config.h>
#endif

#include <stdio.h>
#include <sys/types.h>

#ifdef HAVE_STDLIB_H
#  include <stdlib.h>
#else
extern void exit();
#endif

#ifdef READLINE_LIBRARY
#  include "readline.h"
#  include "history.h"
#else
#  include <readline/readline.h>
#  include <readline/history.h>
#endif

extern HIST_ENTRY **history_list ();


JNIEXPORT jbyteArray JNICALL Java_de_uniduesseldorf_dxram_utils_JNIconsole_readline(JNIEnv *p_env, jclass p_class) {
    char *temp, *prompt, *ptr;
    
    
    temp = (char *)NULL;
    
    prompt = "$ ";
    
    temp = readline (prompt);
    
    add_history (temp);
   
    
    int idx,len;
    
    len = strlen(temp);
    
    jbyte buff[len];
    jbyteArray result = (*p_env)->NewByteArray(p_env, len);
    if (result == NULL) {
        free (temp);
        return NULL; /* out of memory error thrown */
    }
    
    
    ptr = temp;
    idx = 0;
    while (*ptr != '\0') {
        buff[idx++] = *ptr;
        ptr++;
    }
    
    free (temp);

    (*p_env)->SetByteArrayRegion(p_env, result, 0, len, buff);
    return result;
}
