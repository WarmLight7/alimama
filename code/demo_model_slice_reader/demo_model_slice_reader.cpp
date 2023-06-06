#include<bits/stdc++.h>
#include"ModelSliceReader.h"
using namespace std;

int main() {
	vector<ModelSliceReader> A(3);
	for (int i = 0; i < 3; i++) {
		cout << A[i].Load("./demo_model_slice_reader.cpp") << endl;
		char buf[128] = {0};
		cout << A[i].Read(i, 20, buf) << endl;
		cout << buf << endl;
	}
}
