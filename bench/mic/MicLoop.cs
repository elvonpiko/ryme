// Records the default Windows microphone while the default speakers play a
// tone. Prints the quiet-window and tone-window RMS. Exit 0 when the tone
// rose above the room, 1 when the mic stayed quiet or could not be opened.
using System;
using System.IO;
using System.Media;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

class MicLoop {
    [DllImport("winmm.dll", CharSet = CharSet.Ansi)]
    static extern int mciSendString(string command, StringBuilder buffer, int bufferLength, IntPtr callback);

    static int Mci(string command) {
        StringBuilder buffer = new StringBuilder(260);
        int err = mciSendString(command, buffer, buffer.Capacity, IntPtr.Zero);
        if (err != 0) {
            Console.Error.WriteLine("mci " + err + " " + command);
        }
        return err;
    }

    static void Main() {
        string dir = Path.GetTempPath();
        string tonePath = Path.Combine(dir, "ryme-tone.wav");
        string recPath = Path.Combine(dir, "ryme-mic.wav");
        WriteTone(tonePath, 16000, 880.0, 0.7, 0.45);
        if (Mci("close rec") != 0) {
            // A leftover alias is fine. The open below is what matters.
        }
        if (Mci("open new type waveaudio alias rec") != 0) {
            Environment.Exit(1);
        }
        Mci("set rec time format ms");
        Mci("set rec format tag pcm");
        Mci("set rec channels 1");
        Mci("set rec samplespersec 16000");
        Mci("set rec bitspersample 16");
        Mci("set rec alignment 2");
        Mci("set rec bytespersec 32000");
        if (Mci("record rec") != 0) {
            Mci("close rec");
            Environment.Exit(1);
        }
        Thread.Sleep(600);
        using (SoundPlayer player = new SoundPlayer(tonePath)) {
            player.PlaySync();
        }
        Thread.Sleep(500);
        Mci("stop rec");
        if (Mci("save rec \"" + recPath + "\"") != 0) {
            Mci("close rec");
            Environment.Exit(1);
        }
        Mci("close rec");
        int code = Report(recPath);
        try { File.Delete(tonePath); } catch { }
        try { File.Delete(recPath); } catch { }
        Environment.Exit(code);
    }

    static void WriteTone(string path, int rate, double hz, double seconds, double amp) {
        int frames = (int)(rate * seconds);
        byte[] data = new byte[frames * 2];
        for (int i = 0; i < frames; i++) {
            short sample = (short)(Math.Sin(2 * Math.PI * hz * i / rate) * amp * 32767);
            data[i * 2] = (byte)(sample & 0xff);
            data[i * 2 + 1] = (byte)((sample >> 8) & 0xff);
        }
        using (FileStream file = File.Create(path))
        using (BinaryWriter writer = new BinaryWriter(file)) {
            writer.Write(System.Text.Encoding.ASCII.GetBytes("RIFF"));
            writer.Write(36 + data.Length);
            writer.Write(System.Text.Encoding.ASCII.GetBytes("WAVE"));
            writer.Write(System.Text.Encoding.ASCII.GetBytes("fmt "));
            writer.Write(16);
            writer.Write((short)1);
            writer.Write((short)1);
            writer.Write(rate);
            writer.Write(rate * 2);
            writer.Write((short)2);
            writer.Write((short)16);
            writer.Write(System.Text.Encoding.ASCII.GetBytes("data"));
            writer.Write(data.Length);
            writer.Write(data);
        }
    }

    static int Report(string path) {
        byte[] bytes = File.ReadAllBytes(path);
        int rate;
        short bits;
        short channels;
        byte[] pcm = Pcm(bytes, out rate, out bits, out channels);
        if (pcm == null || rate <= 0 || channels <= 0) {
            Console.Error.WriteLine("recording was empty");
            return 1;
        }
        double noise = Rms(pcm, rate, bits, channels, 0.10, 0.45);
        double tone = Rms(pcm, rate, bits, channels, 0.75, 1.20);
        bool heard = tone > 0.01 && tone > noise * 2.0;
        Console.WriteLine("rate " + rate);
        Console.WriteLine("bits " + bits);
        Console.WriteLine("noise_rms " + noise.ToString("0.0000"));
        Console.WriteLine("tone_rms " + tone.ToString("0.0000"));
        Console.WriteLine(heard ? "heard yes" : "heard no");
        return heard ? 0 : 1;
    }

    static byte[] Pcm(byte[] wav, out int rate, out short bits, out short channels) {
        rate = 0;
        bits = 0;
        channels = 0;
        int offset = 12;
        byte[] data = null;
        while (offset + 8 <= wav.Length) {
            string id = System.Text.Encoding.ASCII.GetString(wav, offset, 4);
            int size = BitConverter.ToInt32(wav, offset + 4);
            int start = offset + 8;
            if (size < 0 || start + size > wav.Length) break;
            if (id == "fmt " && size >= 16) {
                channels = BitConverter.ToInt16(wav, start + 2);
                rate = BitConverter.ToInt32(wav, start + 4);
                bits = BitConverter.ToInt16(wav, start + 14);
            } else if (id == "data") {
                data = new byte[size];
                Buffer.BlockCopy(wav, start, data, 0, size);
            }
            offset = start + size + (size & 1);
        }
        return data;
    }

    static double Rms(byte[] pcm, int rate, short bits, short channels, double from, double to) {
        int width = bits == 8 ? 1 : 2;
        int frame = Math.Max(1, width * channels);
        int first = (int)(from * rate);
        int last = Math.Min((int)(to * rate), pcm.Length / frame);
        if (last <= first) return 0;
        double sum = 0;
        int count = 0;
        for (int i = first; i < last; i++) {
            int at = i * frame;
            double sample;
            if (bits == 8) {
                sample = (pcm[at] - 128) / 128.0;
            } else {
                if (at + 1 >= pcm.Length) break;
                short value = BitConverter.ToInt16(pcm, at);
                sample = value / 32768.0;
            }
            sum += sample * sample;
            count++;
        }
        if (count == 0) return 0;
        return Math.Sqrt(sum / count);
    }
}
