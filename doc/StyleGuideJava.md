# Java Style Guide

This style guide outlines the style used for all code written in Java in DXRAM and all related projects. We would like
to keep this style consistent across all projects. Instead of writing down all the rules we want you to follow when
contributing to our projects, please use the [formatter.xml](../intellij/formatter.xml) file with IntelliJ included in
the repository.

Furthermore, please use the [inspections.xml](../intellij/inspections.xml) file with IntelliJ. It includes a lot of
very useful warnings that highlight common mistakes and also enforce some rules to enhance overall code quality.

There are a few additional rules that cannot be enforced by the formatter but want you to stick to:

## Empty lines before and after control blocks
Types of control blocks: if, if-else, for, while, do-while

```
// bad
value = 1;
if (value == 0) {
    value = 1;
}
value++;


// good
value = 1;

if (value == 0) {
    value = 1;
}

value++;
```

Comments that describe the control block are ok to "stick" to it:
```
// good
value = 1;

// sets value to 1
if (value == 0) {
    value = 1;
}

value++;
```

## No empty lines at the start and end of methods or classes
```
// bad
int myFunc() {

    int var = 1;
    return var++;

}

// good
int myFunc() {
     int var = 1;
     return var++;
}
```

## All method parameters must be declared final
```
// bad
void myFunc(int p_v1, bool p_v2) {
    // ...
}

// good
void myFunc(final int v1, final bool p_v2) {
    // ...
}
```
