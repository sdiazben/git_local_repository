package adult.data_model;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import adult.avro.Adult;
import com.google.common.collect.Lists;

public class Generator {

	public static String randomWorkingClass(Random rand) {
		List<String> domain = Lists.newArrayList("Private", "Self-emp-not-inc", "Self-emp-inc", "Federal-gov",
				"Local-gov", "State-gov", "Without-pay", "Never-worked");
		return domain.get(rand.nextInt(domain.size()));
	}

	public static String randomEducation(Random rand) {
		List<String> domain = Lists.newArrayList("Bachelors", "Some-college", "11th", "HS-grad", "Prof-school",
				"Assoc-acdm", "Assoc-voc", "9th", "7th-8th", "12th", "Masters", "1st-4th", "10th", "Doctorate",
				"5th-6th", "Preschool");
		return domain.get(rand.nextInt(domain.size()));
	}

	public static String randomMaritalStatus(Random rand) {
		List<String> domain = Lists.newArrayList("Married-civ-spouse", "Divorced", "Never-married",
				"Separated", "Widowed", "Married-spouse-absent", "Married-AF-spouse");
		return domain.get(rand.nextInt(domain.size()));
	}

	public static String randomOccupation(Random rand) {
		List<String> domain = Lists.newArrayList("Tech-support", "Craft-repair", "Other-service", "Sales",
				"Exec-managerial", "Prof-specialty", "Handlers-cleaners", "Machine-op-inspct", "Adm-clerical",
				"Farming-fishing", "Transport-moving", "Priv-house-serv", "Protective-serv", "Armed-ForcesTech-support",
				"Craft-repair", "Other-service", "Sales", "Exec-managerial", "Prof-specialty", "Handlers-cleaners",
				"Machine-op-inspct", "Adm-clerical", "Farming-fishing", "Transport-moving", "Priv-house-serv",
				"Protective-serv", "Armed-Forces");
		return domain.get(rand.nextInt(domain.size()));
	}

	public static String randomRelationship(Random rand) {
		List<String> domain = Lists.newArrayList("Wife", "Own-child", "Husband", "Not-in-family",
				"Other-relative", "Unmarried");
		return domain.get(rand.nextInt(domain.size()));
	}

	public static String randomRace(Random rand) {
		List<String> domain = Lists.newArrayList("White","Asian-Pac-Islander", "Amer-Indian-Eskimo",
				"Other", "Black");
		return domain.get(rand.nextInt(domain.size()));
	}

	public static String randomSex(Random rand) {
		List<String> domain = Lists.newArrayList("Female", "Male");
		return domain.get(rand.nextInt(domain.size()));
	}

	public static String randomNativeCountry(Random rand) {
		List<String> domain = Lists.newArrayList("United-States", "Cambodia", "England", "Puerto-Rico",
				"Canada", "Germany", "Outlying-US(Guam-USVI-etc)", "India", "Japan", "Greece", "South", "China", "Cuba",
				"Iran", "Honduras", "Philippines", "Italy", "Poland", "Jamaica", "Vietnam", "Mexico", "Portugal",
				"Ireland", "France", "Dominican-Republic", "Laos", "Ecuador", "Taiwan", "Haiti", "Columbia", "Hungary",
				"Guatemala", "Nicaragua", "Scotland", "Thailand", "Yugoslavia", "El-Salvador", "Trinadad&Tobago",
				"Peru", "Hong", "Holand-Netherlands");
		return domain.get(rand.nextInt(domain.size()));
	}
			
	public static String randomNumeric(int min, int max, Random rand) {
		return String.valueOf(ThreadLocalRandom.current().nextInt(min, max + 1));
	}

	public static Adult generateNewInstance(long seed) {
		Adult a = new Adult();
		a.setAge(Integer.parseInt(randomNumeric(18,100,new Random(System.currentTimeMillis()))));
		a.setWorkclass(randomWorkingClass(new Random(System.currentTimeMillis())));
		a.setFnlwgt(Integer.parseInt(randomNumeric(1,1000000,new Random(System.currentTimeMillis()))));
		a.setEducation(randomEducation(new Random(System.currentTimeMillis())));
		a.setEducationNum(Integer.parseInt(randomNumeric(0,10,new Random(System.currentTimeMillis()))));
		a.setMaritalStatus(randomMaritalStatus(new Random(System.currentTimeMillis())));
		a.setRelationship(randomRelationship(new Random(System.currentTimeMillis())));
		a.setRace(randomRace(new Random(System.currentTimeMillis())));
		a.setSex(randomSex(new Random(System.currentTimeMillis())));
		a.setCapitalGain(Integer.parseInt(randomNumeric(1,50000,new Random(System.currentTimeMillis()))));
		a.setCapitalLoss(Integer.parseInt(randomNumeric(1,50000,new Random(System.currentTimeMillis()))));
		a.setHoursPerWeek(Integer.parseInt(randomNumeric(20,40,new Random(System.currentTimeMillis()))));
		a.setNativeCountry(randomNativeCountry(new Random(System.currentTimeMillis())));
		return a;
	}
}
