import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import NavBar from "@/components/NavBar";

export const metadata: Metadata = {
  title: "Food Friendly Program Data Validator",
  description: "Validate and clean NID and DOB data from Excel sheets for the Food Friendly Program (FFP) — Directorate General of Food, Bangladesh",
  authors: [{ name: "Fayez Ahmed" }],
};

const inter = Inter({ subsets: ["latin"] });

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className={`${inter.className} bg-[#0a0a0b]`}>
        <NavBar />
        <main>
          {children}
        </main>
      </body>
    </html>
  );
}
