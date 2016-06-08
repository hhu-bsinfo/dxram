// compile:
// linux:
// gcc -O2 -shared -fpic -o libJNIconsole.so -I/usr/lib/jvm/java-8-openjdk/include/ -I/usr/lib/jvm/java-8-openjdk/include/linux JNIconsole.c -lreadline
// mac:

// gcc -O2 -shared -fpic -o libJNIconsole.dylib -I/Library/Java/JavaVirtualMachines/jdk1.8.0_73.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_73.jdk/Contents/Home/include/darwin JNIconsole.c -lreadline

//// mike:
// gcc -O2 -shared -fpic -o libJNIconsole.dylib -I/Library/Java/JavaVirtualMachines/jdk1.8.0_73.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_73.jdk/Contents/Home/include/darwin JNIconsole.c -lreadline -I/opt/local/include -L/opt/local/lib
// gcc -O2 -shared -fpic -o libJNIconsole.dylib -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include -I/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/include/darwin JNIconsole.c -lreadline -I/opt/local/include/ -L/opt/local/lib


#include <jni.h>
#include <stdlib.h>

#if defined (HAVE_CONFIG_H)
#include <config.h>
#endif

#include <stdio.h>
#include <sys/types.h>
#include <string.h>

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

static int g_autocomplete_cmd_count = 0;
static char** g_autocomplete_cmds = NULL;

static char* dupstr(char* s) 
{
	char *r;

	r = (char*) malloc((strlen(s) + 1));
	strcpy (r, s);
	return (r);
}

static char* autocomplete_generator(const char* text, int state)
{
    static int list_index, len;
    char *name;

    if (!state) {
        list_index = 0;
        len = strlen (text);
    }

    while (name = g_autocomplete_cmds[list_index]) {
        list_index++;

        if (strncmp (name, text, len) == 0)
            return (dupstr(name));
    }

    // If no names matched, then return NULL.
    return ((char *)NULL);
}

static char** autocompletion(const char* text, int start, int end)
{
    char **matches;

    matches = (char **)NULL;

    if (start == 0)
        matches = rl_completion_matches((char*)text, &autocomplete_generator);
    else
        rl_bind_key('\t',rl_abort);

    return (matches);

}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNIconsole_autocompleteCommands(JNIEnv *p_env, jclass p_class, jobjectArray p_commands) {
 	int stringCount = (*p_env)->GetArrayLength(p_env, p_commands);

	if (g_autocomplete_cmds != NULL) {
		for (int i = 0; i < g_autocomplete_cmd_count; i++) {
			free(g_autocomplete_cmds[i]);
		}

		free(g_autocomplete_cmds);
		g_autocomplete_cmds = NULL;
	}
	
	g_autocomplete_cmds = (char**) malloc(sizeof(char*) * stringCount + 1);
	// terminate
	g_autocomplete_cmds[stringCount] = (char*) malloc(sizeof(char) * 1);
	g_autocomplete_cmds[stringCount] = '\0';

    for (int i = 0; i < stringCount; i++) {
        jstring string = (jstring) (*p_env)->GetObjectArrayElement(p_env, p_commands, i);
        const char *rawString = (*p_env)->GetStringUTFChars(p_env, string, 0);
		int len = strlen(rawString);

		g_autocomplete_cmds[i] = (char*) malloc(sizeof(char) * (len + 1)); // +1 null terminator
		strcpy(g_autocomplete_cmds[i], rawString);

		(*p_env)->ReleaseStringUTFChars(p_env, p_commands, rawString);
		(*p_env)->DeleteLocalRef(p_env, string);
    }

	g_autocomplete_cmd_count = stringCount;

	// bind autocomplete callback and enable for tab key
	rl_attempted_completion_function = autocompletion;
	rl_bind_key('\t', rl_complete);
}

JNIEXPORT void JNICALL Java_de_hhu_bsinfo_utils_JNIconsole_addToHistory(JNIEnv *p_env, jclass p_class, jstring p_str) {
    const char* str = (*p_env)->GetStringUTFChars(p_env, p_str, NULL);
	add_history (str);
	printf("%s\n");
}

JNIEXPORT jbyteArray JNICALL Java_de_hhu_bsinfo_utils_JNIconsole_readline(JNIEnv *p_env, jclass p_class, jstring p_prompt) {
    char *temp, *ptr;
	const char* prompt;
    
    
    temp = (char *)NULL;
    
    prompt = (*p_env)->GetStringUTFChars(p_env, p_prompt, NULL);
    
    temp = readline (prompt);
    
	// don't add empty lines to history
	if (temp[0] != '\0') {
    	add_history (temp);
	}
   
    
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


