Name: hamake
Summary: Hamake is a lightweight utility and workflow engine for Hadoop.
Version: 1.0
Release: 0
Group: Applications/MapReduce
License: ASL 2.0
#Requires: hadoop >= 0.18
#Requires:  java >= 1.6.0
URL: http://code.google.com/p/hamake
%description
Hamake is a lightweight utility and workflow engine for Hadoop. Hamake helps to organize your Hadoop Map Reduce jobs, Pig script and local programs in a workflow and launch them based on dataflow principles - your tasks will be executed$

%install
mkdir -p $RPM_BUILD_ROOT%{_libdir}
mkdir -p $RPM_BUILD_ROOT%{_prefix}/local
cp -r ../usr/lib/%{name} $RPM_BUILD_ROOT%{_libdir}
cp -r ../usr/local/%{name} $RPM_BUILD_ROOT%{_prefix}/local
#chmod ug+x $RPM_BUILD_ROOT%{_prefix}/local/%{name}/bin/*

%clean
rm -drf %{_libdir}/%{name}
rm -drf %{_prefix}/local/%{name}

%files
%defattr(-,root,root,-)
%{_libdir}/%{name}/hamake-1.0.jar
%{_prefix}/local/%{name}/hamake-examples-1.0.jar
%{_prefix}/local/%{name}/hamakefiles/class-size.xml
%{_prefix}/local/%{name}/hamakefiles/class-size-s3.xml
%{_prefix}/local/%{name}/scripts/median.pig
%{_prefix}/local/%{name}/bin/*
%attr(-,hamake,hamake) %{_prefix}/local/%{name}
%attr(0755,hamake,hamake) %{_prefix}/local/%{name}/bin/*

%changelog
* Tue Apr 27 2010 Bondar Alex <abondar@codeminders.com> 1.0.0
- Initial version
