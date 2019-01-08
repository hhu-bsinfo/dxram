# Java Style Guide

This style guide outlines the style used for all code written in Java in DXRAM and all related projects. We would like
to keep this style consistent across all projects. Instead of writing down all the rules we want you to follow when
contributing to our projects, please use the [formatter.xml](../intellij/formatter.xml) and
[checkstyle.xml](../intellij/checkstyle.xml) files with IntelliJ included in the repository. You can setup macros
to apply the formatter rules when saving Java-files (e.g. on *ctrl + s*).

Furthermore, please use the [inspections.xml](../intellij/inspections.xml) file with IntelliJ. It includes a lot of
very useful warnings that highlight common mistakes and also enforce some rules to enhance overall code quality.

There are a few additional rules that cannot be enforced by the formatter but want you to stick to:

## Empty lines before and after control blocks
Types of control blocks: if, if-else, for, while, do-while, try-catch

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

## No empty lines on nested control blocks
```
// bad
if (var == 1) {

    while (true) {
        // ...
    }

}

// good
if (var == 1) {
    while (true) {
        // ...
    }
}

// good
if (var == 1) {
    // comment
    while (true) {
        // ...
    }
}
```

However, if the control blocks starts with a statement, make sure to add an empty line afterwards if followed by
another control block.

```
// bad
if (var == 1) {
    int var2 = 2;
    while (true) {
        // ...
    }
}

// good
if (var == 1) {
    int var2 = 2;

    while (true) {
        // ...
    }
}

// good
if (var == 1) {
    int var2 = 2;

    // comment
    while (true) {
        // ...
    }
}
```

## Fill up line on method arguments also when exceeding a single line
```
// bad
public void method(
        final int p_v1,
        final int p_v2,
        final int p_v3,
        final int p_v4) {
    // ...
}

// good
public void method(final int p_v1, final int p_v2, final int p_v3, final int p_v4) {
    // ...
}

// good
public void method(final int p_v1, final int p_v2, final int p_v3, final int p_v4, final int p_v5, final int p_v6,
        final int p_v7, final int p_v8) {
    // ...
}
```

## Fill up line on method calls when passing parameters
```
// bad
myVeryLongLongLongLongMethodName(
        myLongVariableName1,
        myLongVariableName2,
        myLongVariableName3,
        myLongVariableName4,
        myLongVariableName5,
        myLongVariableName6);

// good
myVeryLongLongLongLongMethodName(myLongVariableName1, myLongVariableName2, myLongVariableName3, myLongVariableName4,
        myLongVariableName5, myLongVariableName6);
```
