package test;

import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Test;

public class PatternTest {

  @Test
  public void testPatterns() {
    String input0 = "0";
    String input1 = "9";
    String input2 = "23";
    String input3 = "9772";
    String input4 = "-324";
    String input5 = "54";
    String input6 = "-23432456";
    String input7 = "13457143587";

    Pattern intPattern = Pattern.compile("^(\\+|-)?\\d+$");

    boolean r0 = intPattern.matcher(input0).matches();
    boolean r1 = intPattern.matcher(input1).matches();
    boolean r2 = intPattern.matcher(input2).matches();
    boolean r3 = intPattern.matcher(input3).matches();
    boolean r4 = intPattern.matcher(input4).matches();
    boolean r5 = intPattern.matcher(input5).matches();
    boolean r6 = intPattern.matcher(input6).matches();
    boolean r7 = intPattern.matcher(input7).matches();

    assertTrue("0 - OK", r0);
    assertTrue("1 - OK", r1);
    assertTrue("2 - OK", r2);
    assertTrue("3 - OK", r3);
    assertTrue("4 - OK", r4);
    assertTrue("5 - OK", r5);
    assertTrue("6 - OK", r6);
    assertTrue("7 - OK", r7);

    Pattern doublePattern = Pattern.compile(
        "[\\x00-\\x20]*[+-]?(NaN|Infinity|((((\\p{Digit}+)(\\.)?((\\p{Digit}+)?)"
        +
        "([eE][+-]?(\\p{Digit}+))?)|(\\.((\\p{Digit}+))([eE][+-]?(\\p{Digit}+))?)|"
        +
        "(((0[xX](\\p{XDigit}+)(\\.)?)|(0[xX](\\p{XDigit}+)?(\\.)(\\p{XDigit}+)))"
        + "[pP][+-]?(\\p{Digit}+)))[fFdD]?))[\\x00-\\x20]*");

    String input8 = "-3";
    String input9 = "-4.5";
    String input10 = "83.324";
    String input11 = "45f";
    String input12 = "-324235.4558294";
    String input13 = "0.0001";
    String input14 = "-23432456";
    String input15 = "234.32452";

    boolean r8 = doublePattern.matcher(input8).matches();
    boolean r9 = doublePattern.matcher(input9).matches();
    boolean r10 = doublePattern.matcher(input10).matches();
    boolean r11 = doublePattern.matcher(input11).matches();
    boolean r12 = doublePattern.matcher(input12).matches();
    boolean r13 = doublePattern.matcher(input13).matches();
    boolean r14 = doublePattern.matcher(input14).matches();
    boolean r15 = doublePattern.matcher(input15).matches();

    assertTrue("8 - OK", r8);
    assertTrue("9 - OK", r9);
    assertTrue("10 - OK", r10);
    assertTrue("11 - OK", r11);
    assertTrue("12 - OK", r12);
    assertTrue("13 - OK", r13);
    assertTrue("14 - OK", r14);
    assertTrue("15 - OK", r15);
  }
}
