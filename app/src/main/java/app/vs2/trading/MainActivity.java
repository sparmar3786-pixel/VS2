package app.vs2.trading;

import android.app.*;import android.os.*;import android.content.*;import android.graphics.Color;import android.view.*;import android.webkit.*;import android.widget.*;

public class MainActivity extends Activity {
  WebView web; android.content.SharedPreferences prefs;
  @Override public void onCreate(Bundle b){super.onCreate(b); prefs=getSharedPreferences("vs2",0); showWeb();}
  void showWeb(){
    web=new WebView(this); web.setBackgroundColor(Color.rgb(11,14,20));
    WebSettings s=web.getSettings(); s.setJavaScriptEnabled(true); s.setDomStorageEnabled(true); s.setAllowFileAccess(true); s.setMediaPlaybackRequiresUserGesture(false); s.setSupportZoom(false);
    web.setWebViewClient(new WebViewClient(){@Override public boolean shouldOverrideUrlLoading(WebView v,String u){v.loadUrl(u);return true;}});
    setContentView(web); String url=prefs.getString("server_url",""); if(url.isEmpty()) askServer(); else load(url);
  }
  void askServer(){ final EditText e=new EditText(this); e.setHint("http://192.168.1.10:8000"); e.setSingleLine(); e.setText("http://127.0.0.1:8000");
    new AlertDialog.Builder(this).setTitle("VS2 Trade AI").setMessage("Backend URL enter karein. Android phone par backend chal raha ho to PC ka LAN IP use karein.").setView(e).setCancelable(false).setPositiveButton("Connect",(d,w)->{String u=e.getText().toString().trim(); if(!u.startsWith("http"))u="http://"+u; prefs.edit().putString("server_url",u).apply();load(u);}).show(); }
  void load(String u){ if(!u.endsWith("/"))u+="/"; web.loadUrl(u); }
  @Override public void onBackPressed(){if(web!=null&&web.canGoBack())web.goBack();else super.onBackPressed();}
}
