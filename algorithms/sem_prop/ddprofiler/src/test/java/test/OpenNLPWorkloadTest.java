package test;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.List;
import java.util.Vector;

import org.junit.Test;

import analysis.modules.EntityAnalyzer;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;

public class OpenNLPWorkloadTest {

  // Person model can identify Raul, but cannot identify others.
  private String[] personArr = {"Raul", "Zia", "Sibo", "Dong", ""};

  private String[] personArr1 = {"Sibo", "Dong", "Zia", ""};

  // Person model cannot identify pattern with both first name and last name
  private String[] personArr2 = {
      "Sibo Wang",  "Ziawasch Abedjan", "Raul Castro Fernandez",
      "Nan Tang",   "Mourad Ouzzani",   "Miachel Stonebraker",
      "Sam Madden", "Ahmed Elmagarmid "};

  // cannot identify any of the address below
  private String[] locationArr = {
      "BLK 265, Singapore", "77 Massachusetts Ave, Cambridge, MA 02139",
      "Building EN 7 Room EN 704 Einsteinufer 17 10587 Berlin", "New York",
  };

  // cannot recognize any of the following organizations
  private String[] organizationArr = {"MIT", "NTU", "NUS"};

  // can recognize the following companies.
  private String[] organizationArr1 = {"IBM", "Microsoft", "Google"};

  // cannot recognize any of the following money
  private String[] moneyArr = {"USD", "3 Euro", "4 SGD"};

  // cannot recognize any of the following money
  private String[] moneyArr1 = {"$2", "€ 3", "SGD $"};

  // recognize the first format and the second format but not the third
  private String[] moneyArr2 = {"$ 2", "SGD 3", "$"};

  // recognize this one
  private String[] moneyArr3 = {"$"};

  private String[] percentageArr = {"30%", "40%", "30 percent"};

  private String[] dateArr = {"22/06/2016", "22/Jun", "22-06-2016"};

  @Test
  public void Workloadtest() {
    EntityAnalyzer ea = new EntityAnalyzer();
    Tokenizer tok = new SimpleTokenizer();

    List<String> personList = new Vector<String>();
    Collections.addAll(personList, personArr);
    ea.feedTextData(personList);
    System.out.println(ea.getEntities());
    ea.clear();

    personList.clear();
    Collections.addAll(personList, personArr1);

    ea.feedTextData(personList);
    System.out.println(ea.getEntities());
    ea.clear();

    personList.clear();
    Collections.addAll(personList, personArr2);
    ea.feedTextData(personList);
    System.out.println(ea.getEntities());
    ea.clear();

    personList.clear();
    for (int i = 0; i < personArr2.length; i++) {
      Collections.addAll(personList, tok.tokenize(personArr2[i]));
    }
    ea.feedTextData(personList);
    System.out.println(ea.getEntities());
    ea.clear();

    List<String> locationList = new Vector<String>();
    Collections.addAll(locationList, locationArr);
    ea.feedTextData(locationList);
    System.out.println(ea.getEntities());
    ea.clear();

    locationList.clear();
    for (int i = 0; i < locationArr.length; i++) {
      Collections.addAll(locationList, locationArr[i].split(" "));
    }
    ea.feedTextData(locationList);
    System.out.println(ea.getEntities());
    ea.clear();

    List<String> organizationList = new Vector<String>();
    Collections.addAll(organizationList, organizationArr);
    ea.feedTextData(organizationList);
    System.out.println(ea.getEntities());
    ea.clear();

    organizationList.clear();
    Collections.addAll(organizationList, organizationArr1);
    ea.feedTextData(organizationList);
    System.out.println(ea.getEntities());
    ea.clear();

    List<String> moneyList = new Vector<String>();
    Collections.addAll(moneyList, moneyArr);
    ea.feedTextData(moneyList);
    System.out.println(ea.getEntities());
    ea.clear();

    moneyList.clear();
    for (int i = 0; i < moneyArr.length; i++)
      Collections.addAll(moneyList, moneyArr[i].split(" "));
    ea.feedTextData(moneyList);
    System.out.println(ea.getEntities());
    ea.clear();

    moneyList.clear();
    Collections.addAll(moneyList, moneyArr1);
    ea.feedTextData(moneyList);
    System.out.println(ea.getEntities());
    ea.clear();

    moneyList.clear();
    for (int i = 0; i < moneyArr1.length; i++)
      Collections.addAll(moneyList, moneyArr1[i].split(" "));
    ea.feedTextData(moneyList);
    System.out.println(ea.getEntities());
    ea.clear();

    moneyList.clear();
    for (int i = 0; i < moneyArr2.length; i++)
      Collections.addAll(moneyList, moneyArr2[i].split(" "));
    ea.feedTextData(moneyList);
    System.out.println(ea.getEntities());
    ea.clear();

    moneyList.clear();
    Collections.addAll(moneyList, moneyArr3);
    ea.feedTextData(moneyList);
    System.out.println(ea.getEntities());
    ea.clear();

    List<String> percentageList = new Vector<String>();
    Collections.addAll(percentageList, percentageArr);
    ea.feedTextData(percentageList);
    System.out.println(ea.getEntities());
    ea.clear();

    percentageList.clear();
    for (int i = 0; i < percentageArr.length; i++)
      Collections.addAll(percentageList, percentageArr[i].split(" "));
    ea.feedTextData(percentageList);
    System.out.println(ea.getEntities());
    ea.clear();

    List<String> dateList = new Vector<String>();
    Collections.addAll(dateList, dateArr);
    ea.feedTextData(dateList);
    System.out.println(ea.getEntities());
    ea.clear();

    dateList.clear();
    for (int i = 0; i < dateArr.length; i++) {
      System.out.println();
      String[] tokens = tok.tokenize(dateArr[i]);
      for (int j = 0; j < tokens.length; j++) {
        System.out.println(tokens[j]);
      }
      Collections.addAll(dateList, tokens);
    }
    ea.feedTextData(dateList);
    System.out.println(ea.getEntities());
    ea.clear();
  }
}
