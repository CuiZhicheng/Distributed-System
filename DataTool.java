import java.io.*;
import java.util.*;

public class DataTool{

	public static Set<String> ReadVariable(String filepath) {
		File file = new File(filepath);
		BufferedReader reader = null;
		Set<String> result = new HashSet<>();
		try{
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while((tempString = reader.readLine()) != null){
				String[] strings = tempString.split(" ");
				String key = strings[0];
				String value = strings[1];
				result.add(key);
				result.add(value);
			}
			reader.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}

	//从约定的table保存格式中读取table
	public static HashMap<String, ArrayList<String> > readFile(String filepath){
		File file = new File(filepath);
		BufferedReader reader = null;
		HashMap<String, ArrayList<String>> result = new HashMap<>();
		try{
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while((tempString = reader.readLine()) != null){
				String[] strings = tempString.split(" ");
				String key = strings[0];
				String[] temp = Arrays.copyOfRange(strings, 1, strings.length);
				result.put(key, (ArrayList<String>)Arrays.asList(temp));
			}
			reader.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}

	//record the lines of the file
	public static int readLineFile(String filepath) {
		File file = new File(filepath);
		BufferedReader reader = null;
		String str;
		int Line = 0;
		try {
			reader = new BufferedReader(new FileReader(file));
			while ((str = reader.readLine()) != null) {
				Line++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return Line;
	}

	//从原始数据格式中读取table
	public static HashMap<String, ArrayList<String>> readOriginFile(String filepath) {
		File file = new File(filepath);
		BufferedReader reader = null;
		HashMap<String, ArrayList<String>> result = new HashMap<>();
		try{
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			while((tempString = reader.readLine()) != null){
				String[] strings = tempString.split(" ");
				String key = strings[0];
				String value = strings[1];
				if(!result.containsKey(key)){
					ArrayList<String>  tempInt = new ArrayList<>();
					tempInt.add(value);
					result.put(key, tempInt);
				}else{
					result.get(key).add(value);
				}
			}
			reader.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}

	//将table写入到文件
	public static void writeFile(HashMap<String, ArrayList<String> > table, String outputPath){
		try {
			FileWriter fw = new FileWriter(outputPath);
			for(HashMap.Entry<String, ArrayList<String>> entry: table.entrySet()){
				String key = entry.getKey();
				ArrayList<String> value = entry.getValue();
				fw.write(key);
				fw.write(" ");
				for(int i = 0; i < value.size() - 1; i++){
					fw.write(value.get(i));
					fw.write(" ");
				}
				if(value.size() - 1 >= 0){
					fw.write(value.get(value.size() - 1));
				}
				fw.write("\n");
			}
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static HashMap<String, Double> getInitialVector(HashMap<String, ArrayList<String> > table){
		HashMap<String, Double> result = new HashMap<>();
		for(HashMap.Entry<String, ArrayList<String>> entry: table.entrySet()){
			String key = entry.getKey();
			if(!result.containsKey(key)){
				result.put(key, 0.0);
			}
			ArrayList<String> value = entry.getValue();
			for (String i: value) {
				if(!result.containsKey(i)){
					result.put(i, 0.0);
				}
			}
		}
		return result;
	}

	// return the information of the worker
	//the first line of CheckPoint0/1
	//WorkerId StartLine EndLine
	public static HashMap<String, Integer> readInfo(String filePath) {
		File file = new File(filePath);
		BufferedReader reader = null;
		HashMap<String, Integer> result = new HashMap<>();
		try{
			reader = new BufferedReader(new FileReader(file));
			String tempString = reader.readLine();
			int count = Integer.parseInt(tempString);
			tempString = reader.readLine();
			String[] strs = tempString.split(" ");
			int workerid = Integer.parseInt(strs[0]);
			int startline = Integer.parseInt(strs[1]);
			int endline = Integer.parseInt(strs[2]);
			result.put("Count", count);
			result.put("WorkerId", workerid);
			result.put("StartLine", startline);
			result.put("EndLine", endline);
			reader.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
	}


	//保存checkPoint
	public static void writeVector(int Count, int WorkerId, int StartLine, int EndLine, HashMap<String, Double> current, String filePath){
		try {
			File f = new File(filePath);
			if (!f.exists())
				f.createNewFile();
			FileWriter fw = new FileWriter(filePath);
			String info = String.valueOf(Count);
			fw.write(info + "\n");
			info = String.valueOf(WorkerId) + " " + String.valueOf(StartLine) + " " + String.valueOf(EndLine);
			fw.write(info + "\n");
			for(Map.Entry<String, Double> entry: current.entrySet()){
				fw.write(entry.getKey() + " ");
				fw.write(entry.getValue() + "\n");
			}
			fw.close();
		}catch(IOException e){
			e.printStackTrace();
		}
	}

	//从checkPoint中读取当前数组
	public static HashMap<String, Double> readVector(String filePath){
			File file = new File(filePath);
			BufferedReader reader = null;
			HashMap<String, Double> result = new HashMap<>();
			try{
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				tempString = reader.readLine();
				tempString = reader.readLine();
				while((tempString = reader.readLine()) != null){
					String[] strings = tempString.split(" ");
					String key = strings[0];
					Double value = Double.parseDouble(strings[1]);
					result.put(key, value);
				}
				reader.close();
			}catch(Exception e){
				e.printStackTrace();
			}
			return result;
	}
};